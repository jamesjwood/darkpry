#! /usr/bin/env node

// EXTERNAL DEPENDENCIES
import * as dotenv from 'dotenv'
import * as fs from 'fs'
import * as csv from 'csv-parser'
import * as stream from 'stream'
import {Transform} from 'json2csv'

// INTERNAL DEPENDENCIES
import darkSkyDataTransform from './darkSkyDataTransform'
import FlattenHoursTransform from './FlattenHoursTransform'

dotenv.config()

// CONSTANTS
const DARK_SKY_TOKEN = process.env.DARK_SKY_TOKEN
const DARK_SKY_HOSTNAME = 'api.darksky.net'
const DARK_SKY_PATH = 'forecast'
const DARK_SKY_PROTOCOL = 'https'
const INPUT_FILE_NAME = './input.csv'
const CSV_OUTPUT_FILE_NAME = './out/output.csv'
const DARK_SKY_CONCURRENT_CONNECTIONS = 100

// const csvFields = ['latitude', 'longitude', 'summary'] // Use this if we only want specific fields
const csvOptions = {} // { csvFields }
const csvTransformOptions = { highWaterMark: 8192, objectMode: true }
const toCSVTransform = new Transform(csvOptions, csvTransformOptions)

const readStream = fs.createReadStream(INPUT_FILE_NAME)
const writeStream = fs.createWriteStream(CSV_OUTPUT_FILE_NAME)

stream.pipeline(
  readStream,
  csv(),
  darkSkyDataTransform(DARK_SKY_HOSTNAME, DARK_SKY_PROTOCOL, DARK_SKY_PATH, DARK_SKY_TOKEN, DARK_SKY_CONCURRENT_CONNECTIONS),
  new FlattenHoursTransform(),
  toCSVTransform,
  writeStream,
  (error) => {
    if (error) {
      console.error('Pipeline failed', error)
    } else {
      console.log('Pipeline succeeded')
    }
  })
