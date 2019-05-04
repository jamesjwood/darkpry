import * as dot from "dot-object";
import * as stream from "stream";
import * as types from "./types";

class FlattenHoursTransform extends stream.Transform {
  constructor() {
    super({
      objectMode: true,
      writableObjectMode: true,
    });
  }

  public _transform(chunk, encoding, cb) {
    const flattened = flattenHours(chunk);
    flattened.map((row) => {
      this.push(row);
    });
    cb();
  }

  public _flush(cb) {
    cb();
  }
}

const flattenHours = (darkSkyCoordinateJsonAsObject: types.IDarkSkyResponse) => {
  const flattened = [];
  darkSkyCoordinateJsonAsObject.hourly.data.map((hour) => {
    flattened.push(dot.dot({
      day: darkSkyCoordinateJsonAsObject.daily.data[0],
      hour,
      latitude: darkSkyCoordinateJsonAsObject.latitude,
      longitude: darkSkyCoordinateJsonAsObject.longitude,
    }));
  });
  return flattened;
};

export default FlattenHoursTransform;
