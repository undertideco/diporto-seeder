require('dotenv').config();
const pg = require('pg');
const Rx = require('rx');
const coordinates = require('./coordinates.json');

const GOOGLE_PLACES_RADIUS = 600;

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

// Utility function for flatMap use only. Returns first parameter.
const useFirstParam = a => a;

// Utility function for reduce use only. Returns addition.
const reduceSum = (a, b) => a + b;

// Get an existing category's ID, or inserts a new one and returns the new ID.
const categoryIdFirstOrCreate = name => pool.query('INSERT INTO category (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = $1 RETURNING id', [name])
  .then(insertResult => insertResult.rows[0].id);

const userIdFirstOrCreate = name => pool.query('INSERT INTO "user" (user_name,name,password_hash,is_admin) VALUES ($1,$2,\'\',false) ON CONFLICT (user_name) DO UPDATE SET user_name = $1 RETURNING id', [name.replace(' ', '').toLowerCase(), name])
  .then(insertResult => insertResult.rows[0].id);

Rx.Observable.fromArray(coordinates)
  .delay(200)
  .flatMap(({ lat, lon }) => Rx.Observable.fromPromise(
    // Get nearby places
    googleMapsClient.placesNearby({
      location: [lat, lon],
      radius: GOOGLE_PLACES_RADIUS,
      // type: 'cafe|restaurant',
    }).asPromise()
      .then(resp => resp.json.results),
  ))
  .flatMap(Rx.Observable.fromArray)
  .flatMap(googleNearbyPlace => Rx.Observable.fromPromise(
    // Get more details of the place
    googleMapsClient.place({ placeid: googleNearbyPlace.place_id })
      .asPromise()
      .then(resp => resp.json.result),
  ), Object.assign)
  .map(place => Object.assign(place, { // Patch for happiness
    photos: place.photos || [],
    reviews: place.reviews || [],
  }))
  .doOnNext(place => console.log(`Processing ${place.name}`))
  .flatMap(place => Rx.Observable.fromPromise(
    // Insert place into database
    pool.query(`
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
      ]).then(result => Object.assign(place, { dbID: result.rows[0].id })),
  ))
  .doOnNext(place => console.log(`Inserting pc ${place.name}`))
  .flatMap(place => Rx.Observable.fromArray(place.types).flatMap(type => Rx.Observable.fromPromise(
    // Deal with place_category
    categoryIdFirstOrCreate(type).then(categoryDBID =>
      pool.query('INSERT INTO place_category(place_id,category_id) VALUES ($1,$2) ON CONFLICT DO NOTHING',
        [place.dbID, categoryDBID],
      ),
    ),
  ).retry(1), useFirstParam).defaultIfEmpty(9999).reduce(reduceSum), useFirstParam)
  .doOnNext(place => console.log(`Inserting photo ${place.name}`))
  .flatMap(place => Rx.Observable.fromArray(place.photos)
    .flatMap(photo => Rx.Observable.fromPromise(
      pool.query(
        'INSERT INTO place_photo(url,place_id) VALUES ($1,$2) ON CONFLICT DO NOTHING',
        [photo.photo_reference, place.dbID],
      ),
    ), useFirstParam).defaultIfEmpty(9999).reduce(reduceSum), useFirstParam)
  .doOnNext(place => console.log(`Inserting review ${place.name}`))
  .flatMap(place => Rx.Observable.fromArray(place.reviews)
    .flatMap(review => Rx.Observable.fromPromise(
      userIdFirstOrCreate(review.author_name).then(userID => pool.query(
        'INSERT INTO place_review(place_id,rating,text,time,user_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING',
        [
          place.dbID,
          review.rating,
          review.text,
          new Date(review.time * 1000),
          userID,
        ],
      )),
    ), useFirstParam).defaultIfEmpty(9999).reduce(reduceSum), useFirstParam)
  .subscribe(place => console.log(`Completed ${place.name}`), console.error, () => {
    pool.end();
  });
