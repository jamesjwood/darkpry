
import fetch from 'node-fetch'
import * as through2Concurrent from 'through2-concurrent'

interface IInputRow {
  time: string,
  long: string,
  lat: string
}
const getDarkSkyData = async (hostname: string, protocol: 'http' | 'https', path: string, token: string, coordinate: IInputRow) => {
  console.log(`Getting data for lat: ${coordinate.lat} long: ${coordinate.long} from ${hostname} at ${coordinate.time}`)
  const coordinateTime = coordinate.time
  const formattedRequestURLForCoordinateAsString = `${protocol}://${hostname}/${path}/${token}/${coordinate.lat},${coordinate.long},${coordinateTime}?exclude=flags,minutely,currently&units=si`
  const darkSkyCoordinateResponse = await fetch(formattedRequestURLForCoordinateAsString)
  return darkSkyCoordinateResponse.json()
}

const darkSkyDataTransform = (hostname: string, protocol: 'http' | 'https', path: string, token: string, concurrentConnections: number) => {
  return through2Concurrent.obj(
    { maxConcurrency: concurrentConnections },
    function (chunk, enc, callback) {
      var self = this
      getDarkSkyData(hostname, protocol, path, token, chunk).then((newChunk) => {
        self.push(newChunk)
        callback()
      }).catch(callback)
    })
}

export default darkSkyDataTransform
