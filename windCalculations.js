// Wind calculations module based on the Node-RED flow
class WindCalculations {
  constructor(app) {
    this.app = app;
    this.headingTrue = 0;
    this.headingMagnetic = 0;
    this.courseOverGroundMagnetic = null;
    this.speedOverGround = 0;
    this.airTemp = 0;
    this.humidity = 0;
    this.anchorSet = false;
    this.anchorApparentBearing = 0;
  }

  // Helper function to convert degrees to radians
  degToRad(deg) {
    return deg * Math.PI / 180;
  }

  // Helper function to convert radians to degrees
  radToDeg(rad) {
    return rad * 180 / Math.PI;
  }

  // Helper function: normalize angle to [-π, π]
  normalizeAngle(angle) {
    while (angle > Math.PI) angle -= 2 * Math.PI;
    while (angle < -Math.PI) angle += 2 * Math.PI;
    return angle;
  }

  // Helper function to convert atan2 result to compass bearing [0, 2π]
  toCompassBearing(radians) {
    return radians < 0 ? radians + 2 * Math.PI : radians;
  }

  // Update navigation data from SignalK
  updateNavigationData(path, value) {
    switch (path) {
      case 'navigation.headingTrue':
        this.headingTrue = value;
        break;
      case 'navigation.headingMagnetic':
        this.headingMagnetic = value;
        break;
      case 'navigation.courseOverGroundMagnetic':
        this.courseOverGroundMagnetic = value;
        break;
      case 'navigation.speedOverGround':
        this.speedOverGround = value;
        break;
      case 'environment.outside.tempest.observations.airTemperature':
        this.airTemp = value;
        break;
      case 'environment.outside.tempest.observations.relativeHumidity':
        this.humidity = value;
        break;
    }
  }

  // Calculate apparent wind values
  calculateApparentWind(windData) {
    const windRelative = windData.windDirection;
    
    // Calculate apparent wind angles and directions
    const headingTrueDeg = this.radToDeg(this.headingTrue);
    const headingMagneticDeg = this.radToDeg(this.headingMagnetic);
    const courseOverGroundMagneticDeg = this.courseOverGroundMagnetic ? 
      this.radToDeg(this.courseOverGroundMagnetic) : headingMagneticDeg;

    // Wind angle - relative to bow
    const windAngleRelative = windData.windDirection;
    const windAngleRelativeRad = this.degToRad(windData.windDirection);

    // Wind direction - calculate absolute compass direction  
    const apparentTrueDeg = (headingTrueDeg + windData.windDirection) % 360;
    const apparentMagneticDeg = (headingMagneticDeg + windData.windDirection) % 360;

    return {
      windSpeed: windData.windSpeed,
      windAngleRelative,
      windAngleRelativeRad,
      apparentTrueDeg,
      apparentMagneticDeg,
      apparentTrueRad: this.degToRad(apparentTrueDeg),
      apparentMagneticRad: this.degToRad(apparentMagneticDeg),
      airTemperature: windData.airTemperature || this.airTemp
    };
  }

  // Calculate derived wind values (true wind, wind chill, heat index, etc.)
  calculateDerivedWindValues(apparentWindData) {
    const timestamp = new Date().toISOString();
    const source = 'mqtt-weatherflow-derived';

    // Determine which heading to use for calculations
    let useDirection = this.headingMagnetic;
    if (this.courseOverGroundMagnetic != null) {
      useDirection = this.courseOverGroundMagnetic;
    }

    const effectiveHeadingTrueRad = this.anchorSet ? this.anchorApparentBearing : this.headingTrue;
    const effectiveHeadingMagneticRad = this.anchorSet ? this.anchorApparentBearing : this.headingMagnetic;

    // Compute the apparent wind angle relative to the boat
    const angleApparent = this.normalizeAngle(apparentWindData.windAngleRelativeRad || 
      (apparentWindData.apparentTrueRad - effectiveHeadingTrueRad));

    // True Wind Calculation in the True Frame
    const effectiveSOG = this.anchorSet ? 0 : this.speedOverGround;

    const Vx = effectiveSOG * Math.cos(effectiveHeadingTrueRad);
    const Vy = effectiveSOG * Math.sin(effectiveHeadingTrueRad);
    const Ax = apparentWindData.windSpeed * Math.cos(apparentWindData.apparentTrueRad);
    const Ay = apparentWindData.windSpeed * Math.sin(apparentWindData.apparentTrueRad);
    const Wx = Ax + Vx;
    const Wy = Ay + Vy;
    const trueWindSpeed = Math.sqrt(Wx * Wx + Wy * Wy);

    const rawTrueDirection = Math.atan2(Wy, Wx);
    const trueWindDirTrueRad = this.toCompassBearing(rawTrueDirection);
    const angleTrueGround = this.normalizeAngle(trueWindDirTrueRad - effectiveHeadingTrueRad);

    // True Wind Calculation in the Magnetic Frame
    const VxMag = effectiveSOG * Math.cos(effectiveHeadingMagneticRad);
    const VyMag = effectiveSOG * Math.sin(effectiveHeadingMagneticRad);
    const AxMag = apparentWindData.windSpeed * Math.cos(apparentWindData.apparentMagneticRad);
    const AyMag = apparentWindData.windSpeed * Math.sin(apparentWindData.apparentMagneticRad);
    const WxMag = AxMag + VxMag;
    const WyMag = AyMag + VyMag;

    const rawMagneticDirection = Math.atan2(WyMag, WxMag);
    const trueWindDirMagRad = this.toCompassBearing(rawMagneticDirection);
    const angleTrueWater = this.normalizeAngle(trueWindDirMagRad - effectiveHeadingMagneticRad);

    // Wind Chill Calculation (K)
    const airTempC = this.airTemp - 273.15;
    const windSpeedKmh = trueWindSpeed * 3.6;
    let windChillK = null;

    if (airTempC <= 10 && windSpeedKmh > 4.8) {
      const windChillC = 13.12 + 0.6215 * airTempC - 11.37 * Math.pow(windSpeedKmh, 0.16) + 
        0.3965 * airTempC * Math.pow(windSpeedKmh, 0.16);
      windChillK = windChillC + 273.15;
    }

    // Heat Index Calculation (K)
    const airTempF = (airTempC * 9 / 5) + 32;
    let heatIndexK = null;

    if (airTempF >= 80 && this.humidity >= 40) {
      const T = airTempF;
      const R = this.humidity;
      const heatIndexF = -42.379 + 2.04901523 * T + 10.14333127 * R
        - 0.22475541 * T * R - 0.00683783 * T * T
        - 0.05481717 * R * R + 0.00122874 * T * T * R
        + 0.00085282 * T * R * R - 0.00000199 * T * T * R * R;
      const heatIndexC = (heatIndexF - 32) * 5 / 9;
      heatIndexK = heatIndexC + 273.15;
    }

    // Feels Like Calculation (K)
    let feelsLikeK = this.airTemp;
    if (windChillK !== null && airTempC <= 10) {
      feelsLikeK = windChillK;
    } else if (heatIndexK !== null && airTempC >= 27) {
      feelsLikeK = heatIndexK;
    }

    return {
      speedApparent: apparentWindData.windSpeed,
      angleApparent,
      angleTrueGround,
      angleTrueWater,
      directionTrue: trueWindDirTrueRad,
      directionMagnetic: trueWindDirMagRad,
      speedTrue: trueWindSpeed,
      windChill: windChillK,
      heatIndex: heatIndexK,
      feelsLike: feelsLikeK,
      timestamp,
      source
    };
  }

  // Create SignalK deltas for all wind calculations
  createWindDeltas(derivedValues) {
    const deltas = [];
    const windPaths = {
      speedApparent: 'environment.wind.speedApparent',
      angleApparent: 'environment.wind.angleApparent',
      angleTrueGround: 'environment.wind.angleTrueGround',
      angleTrueWater: 'environment.wind.angleTrueWater',
      directionTrue: 'environment.wind.directionTrue',
      directionMagnetic: 'environment.wind.directionMagnetic',
      speedTrue: 'environment.wind.speedTrue'
    };

    const tempestPaths = {
      windChill: 'environment.outside.tempest.observations.windChill',
      heatIndex: 'environment.outside.tempest.observations.heatIndex',
      feelsLike: 'environment.outside.tempest.observations.feelsLike'
    };

    // Create deltas for wind values
    Object.entries(windPaths).forEach(([key, path]) => {
      if (derivedValues[key] !== undefined) {
        deltas.push({
          context: 'vessels.self',
          updates: [{
            $source: derivedValues.source,
            timestamp: derivedValues.timestamp,
            values: [{
              path: path,
              value: derivedValues[key]
            }]
          }]
        });
      }
    });

    // Create deltas for temperature-related values
    Object.entries(tempestPaths).forEach(([key, path]) => {
      if (derivedValues[key] !== undefined && derivedValues[key] !== null) {
        deltas.push({
          context: 'vessels.self',
          updates: [{
            $source: derivedValues.source,
            timestamp: derivedValues.timestamp,
            values: [{
              path: path,
              value: derivedValues[key]
            }]
          }]
        });
      }
    });

    return deltas;
  }
}

module.exports = WindCalculations;