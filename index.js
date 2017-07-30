require('dotenv').config();
const pg = require('pg');
const async = require('async');

/**
 * @typedef {Object} Coordinate
 * @property {string} name Name
 * @property {number} lat Latitude
 * @property {number} lon Longitude
 */

/** @type {Array<Coordinate>} */
const coordinates = require('./coordinates.json');

// Database config
const config = {
  user: process.env.PG_USER,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  max: 50,
  idleTimeoutMillis: 30000,
};

// Database pool
const pool = new pg.Client(config);
pool.on('error', (err) => {
  console.error('idle client error', err.message, err.stack);
});

pool.connect();

// Google Maps client
const googleMapsClient = require('@google/maps').createClient({
  key: process.env.GOOGLE_API_KEY,
  Promise,
});

// Get an existing category's ID, or inserts a new one and returns the new ID.
const categoryIdFirstOrCreate = name => pool.query('INSERT INTO category (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = $1 RETURNING id', [name])
  .then(insertResult => insertResult.rows[0].id);

const userIdFirstOrCreate = name => pool.query('INSERT INTO "user" (user_name,name,password_hash,is_admin) VALUES ($1,$2,\'\',false) ON CONFLICT (user_name) DO UPDATE SET user_name = $1 RETURNING id', [name.replace(/\s/g, '_').toLowerCase(), name])
  .then(insertResult => insertResult.rows[0].id);

console.log(`${coordinates.length} coordinates to process`);

const waterFallDelay = delay => (arg, callback) => {
  setTimeout(() => {
    callback(null, arg);
  }, delay);
};

/**
 * Generate a waterfall callback handler for Google Places' subsequent calls
 * @param {number} lat 
 * @param {number} lon 
 */
const additionalCallback = (lat, lon) => (resp, callback) => {
  if (!('next_page_token' in resp)) {
    callback(null, resp);
    return;
  }
  googleMapsClient.placesNearby({
    location: [lat, lon],
    pagetoken: resp.next_page_token,
    // type: 'cafe|restaurant',
  }).asPromise()
    .then(res => res.json)
    .then((data) => {
      // Delay next call because Google
      callback(null, Object.assign(data, {
        results: data.results.concat(resp.results),
      }));
    })
    .catch(callback);
};

// Work begins
const getAllGooglePlaces = () => new Promise((resolve, reject) => {
  async.concatLimit(coordinates, 6, ({ name, lat, lon }, concatCallback) => {
    console.log(`Fetching nearby places for '${name}'`);
    // Get nearby places
    async.waterfall([
      (firstCallback) => {
        // Initial nearby (20)
        googleMapsClient.placesNearby({
          location: [lat, lon],
          rankby: 'distance',
          // type: 'cafe|restaurant',
        }).asPromise()
          .then(resp => resp.json)
          .then((data) => {
            firstCallback(null, data);
          })
          .catch(firstCallback);
      },
      waterFallDelay(2000),
      additionalCallback(lat, lon),
      waterFallDelay(2000),
      additionalCallback(lat, lon),
    ], (err, googlePlaces) => {
      if (err !== null) {
        concatCallback(err);
        return;
      }
      concatCallback(null, googlePlaces.results);
    });
  }, (err, places) => {
    if (err !== null) {
      reject(err);
      return;
    }

    // Retrieve more details
    async.mapLimit(places, 3, (place, detailsCallback) => {
      console.log(`Fetching additional details for '${place.name}'`);
      googleMapsClient.place({ placeid: place.place_id })
        .asPromise()
        .then(resp => resp.json.result)
        .then((data) => {
          detailsCallback(null, Object.assign(place, data));
        })
        .catch(detailsCallback);
    }, (er, allPlaces) => {
      if (er !== null) {
        reject(er);
        return;
      }
      console.log(`Retrieved ${allPlaces.length} Google places`);
      resolve(allPlaces.map(place => Object.assign(place, {
        photos: place.photos || [],
        reviews: place.reviews || [],
      })));
    });
  });
});

// Insert place into database
const insertPlace = place => pool.query(`
  INSERT INTO place (
    name, address,lat,lon,opening_hours,phone
  ) VALUES ($1,$2,$3,$4,$5,$6) 
  ON CONFLICT (name,address) 
  DO UPDATE SET 
  name = $1, 
  address = $2,
  lat = $3,
  lon = $4,
  opening_hours = $5,
  phone = $6 
  RETURNING id
  `, [
    place.name,
    place.formatted_address,
    place.geometry.location.lat,
    place.geometry.location.lng,
    JSON.stringify(place.opening_hours) || '{}',
    place.international_phone_number || '',
  ])
  .then(result => result.rows[0].id);

const insertCat = (cat, placeDbId) => categoryIdFirstOrCreate(cat)
  .then(categoryDbId =>
    pool.query('INSERT INTO place_category(place_id,category_id) VALUES ($1,$2) ON CONFLICT DO NOTHING',
      [placeDbId, categoryDbId],
    ),
  );

const insertPhoto = (photoRef, placeDbId) => pool.query(
  'INSERT INTO place_photo(google_place_id,place_id,is_google_places_image) VALUES ($1,$2,true) ON CONFLICT DO NOTHING',
  [photoRef, placeDbId],
);

const insertReview = (review, placeDbId) => userIdFirstOrCreate(review.author_name)
  .then(userID => pool.query(
    'INSERT INTO place_review(place_id,rating,text,time,user_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING',
    [
      placeDbId,
      review.rating,
      review.text,
      new Date(review.time * 1000),
      userID,
    ],
  ));

async function work() {
  const places = await getAllGooglePlaces();
  for (const place of places) {
    console.log(`Processing '${place.name}'`);
    const placeDbId = await insertPlace(place);
    // Categories
    await Promise.all(place.types.map(type => insertCat(type, placeDbId)));

    // Photos
    await Promise.all(place.photos.map(photo => insertPhoto(photo.photo_reference, placeDbId)));

    // Reviews
    await Promise.all(place.reviews.map(review => insertReview(review, placeDbId)));
  }
  process.exit();
}

work();

process.on('unhandledRejection', console.error);
