const dgram = require('dgram');
const WebSocket = require('ws');
const fetch = require('node-fetch');
const WindCalculations = require('./windCalculations');

module.exports = function(app) {
  const plugin = {};
  let udpServer;
  let wsConnection;
  let forecastInterval;
  let windyInterval;
  let windCalculations;
  let navigationSubscriptions = [];
  
  // Plugin metadata
  plugin.id = 'zennora-signalk-weatherflow';
  plugin.name = 'WeatherFlow Weather Station Plugin';
  plugin.description = 'Ingests data from WeatherFlow weather stations via UDP, WebSocket, and API';

  // Configuration schema
  plugin.schema = {
    type: 'object',
    required: ['stationId', 'apiToken'],
    properties: {
      stationId: {
        type: 'number',
        title: 'WeatherFlow Station ID',
        description: 'Your WeatherFlow station ID',
        default: 118081
      },
      apiToken: {
        type: 'string',
        title: 'WeatherFlow API Token',
        description: 'Your WeatherFlow API token',
        default: ''
      },
      udpPort: {
        type: 'number',
        title: 'UDP Listen Port',
        description: 'Port to listen for WeatherFlow UDP broadcasts',
        default: 50222
      },
      enableWebSocket: {
        type: 'boolean',
        title: 'Enable WebSocket Connection',
        description: 'Connect to WeatherFlow WebSocket for real-time data',
        default: true
      },
      enableForecast: {
        type: 'boolean',
        title: 'Enable Forecast Data',
        description: 'Fetch forecast data from WeatherFlow API',
        default: true
      },
      forecastInterval: {
        type: 'number',
        title: 'Forecast Update Interval (minutes)',
        description: 'How often to fetch forecast data',
        default: 30
      },
      enableWindCalculations: {
        type: 'boolean',
        title: 'Enable Wind Calculations',
        description: 'Calculate true wind from apparent wind',
        default: true
      },
      deviceId: {
        type: 'number',
        title: 'WeatherFlow Device ID',
        description: 'Your WeatherFlow device ID for WebSocket connection',
        default: 405588
      }
    }
  };

  // Plugin start function
  plugin.start = function(options, restartPlugin) {
    app.debug('Starting WeatherFlow plugin with options:', options);
    
    // Initialize wind calculations if enabled
    if (options.enableWindCalculations) {
      windCalculations = new WindCalculations(app);
      setupNavigationSubscriptions();
    }
    
    // Initialize UDP server for WeatherFlow broadcasts
    if (options.udpPort) {
      startUdpServer(options.udpPort, options);
    }
    
    // Initialize WebSocket connection
    if (options.enableWebSocket && options.apiToken) {
      startWebSocketConnection(options.apiToken, options.deviceId);
    }
    
    // Initialize forecast data fetching
    if (options.enableForecast && options.apiToken && options.stationId) {
      startForecastFetching(options);
    }
    
    app.debug('WeatherFlow plugin started successfully');
  };

  // Plugin stop function
  plugin.stop = function() {
    app.debug('Stopping WeatherFlow plugin');
    
    if (udpServer) {
      udpServer.close();
      udpServer = null;
    }
    
    if (wsConnection) {
      wsConnection.close();
      wsConnection = null;
    }
    
    if (forecastInterval) {
      clearInterval(forecastInterval);
      forecastInterval = null;
    }
    
    if (windyInterval) {
      clearInterval(windyInterval);
      windyInterval = null;
    }
    
    // Clean up navigation subscriptions
    navigationSubscriptions.forEach(unsubscribe => {
      if (typeof unsubscribe === 'function') {
        unsubscribe();
      }
    });
    navigationSubscriptions = [];
    
    app.debug('WeatherFlow plugin stopped');
  };

  // Setup navigation data subscriptions for wind calculations
  function setupNavigationSubscriptions() {
    if (!windCalculations) return;
    
    const subscriptions = [
      'navigation.headingTrue',
      'navigation.headingMagnetic',
      'navigation.courseOverGroundMagnetic',
      'navigation.speedOverGround',
      'environment.outside.tempest.observations.airTemperature',
      'environment.outside.tempest.observations.relativeHumidity'
    ];
    
    subscriptions.forEach(path => {
      const unsubscribe = app.streambundle.getSelfStream(path).onValue(value => {
        if (value && value.value !== undefined) {
          windCalculations.updateNavigationData(path, value.value);
        }
      });
      navigationSubscriptions.push(unsubscribe);
    });
  }

  // Start UDP server for WeatherFlow broadcasts
  function startUdpServer(port, options) {
    udpServer = dgram.createSocket('udp4');
    
    udpServer.on('message', (msg, rinfo) => {
      try {
        const data = JSON.parse(msg.toString());
        processWeatherFlowMessage(data, options);
      } catch (error) {
        app.debug('Error parsing UDP message:', error);
      }
    });
    
    udpServer.on('error', (err) => {
      app.error('UDP server error:', err);
    });
    
    udpServer.bind(port, () => {
      app.debug(`WeatherFlow UDP server listening on port ${port}`);
    });
  }

  // Start WebSocket connection to WeatherFlow
  function startWebSocketConnection(token, deviceId) {
    const wsUrl = `wss://ws.weatherflow.com/swd/data?token=${token}`;
    
    wsConnection = new WebSocket(wsUrl);
    
    wsConnection.on('open', () => {
      app.debug('WeatherFlow WebSocket connected');
      
      // Request data for device
      const request = {
        type: 'listen_start',
        device_id: deviceId || 405588,
        id: Date.now().toString()
      };
      wsConnection.send(JSON.stringify(request));
    });
    
    wsConnection.on('message', (data) => {
      try {
        const message = JSON.parse(data);
        processWebSocketMessage(message);
      } catch (error) {
        app.debug('Error parsing WebSocket message:', error);
      }
    });
    
    wsConnection.on('error', (error) => {
      app.error('WebSocket error:', error);
    });
    
    wsConnection.on('close', () => {
      app.debug('WebSocket connection closed');
      // Implement reconnection logic here if needed
    });
  }

  // Start forecast data fetching
  function startForecastFetching(options) {
    const fetchForecast = async () => {
      try {
        const url = `https://swd.weatherflow.com/swd/rest/better_forecast?station_id=${options.stationId}&token=${options.apiToken}`;
        const response = await fetch(url);
        const data = await response.json();
        processForecastData(data);
      } catch (error) {
        app.error('Error fetching forecast data:', error);
      }
    };
    
    // Fetch immediately
    fetchForecast();
    
    // Set up interval
    const intervalMs = (options.forecastInterval || 30) * 60 * 1000;
    forecastInterval = setInterval(fetchForecast, intervalMs);
  }

  // Process WeatherFlow UDP messages
  function processWeatherFlowMessage(data, options) {
    if (!data.type) return;
    
    switch (data.type) {
      case 'rapid_wind':
        processRapidWind(data, options);
        break;
      case 'obs_st':
        processTempestObservation(data, options);
        break;
      case 'obs_air':
        processAirObservation(data, options);
        break;
      case 'evt_precip':
        processRainEvent(data, options);
        break;
      case 'evt_strike':
        processLightningEvent(data, options);
        break;
      default:
        app.debug('Unknown WeatherFlow message type:', data.type);
    }
  }

  // Process WebSocket messages
  function processWebSocketMessage(data) {
    // Flatten summary and status properties
    if (data.summary && typeof data.summary === 'object') {
      Object.assign(data, data.summary);
      delete data.summary;
    }
    
    if (data.status && typeof data.status === 'object') {
      Object.assign(data, data.status);
      delete data.status;
    }
    
    // Process observation array if present
    if (data.obs && Array.isArray(data.obs) && data.obs.length > 0) {
      const obsArray = data.obs[0];
      const parsedObs = {
        timeEpoch: obsArray[0],
        windLull: obsArray[1],
        windAvg: obsArray[2],
        windGust: obsArray[3],
        windDirection: obsArray[4],
        windSampleInterval: obsArray[5],
        stationPressure: obsArray[6] * 100, // MB to Pa
        airTemperature: obsArray[7] + 273.15, // °C to K
        relativeHumidity: obsArray[8],
        illuminance: obsArray[9],
        uvIndex: obsArray[10],
        solarRadiation: obsArray[11],
        rainAccumulated: obsArray[12],
        precipitationType: obsArray[13],
        lightningStrikeAvgDistance: obsArray[14] * 1000, // km to m
        lightningStrikeCount: obsArray[15],
        battery: obsArray[16],
        reportInterval: obsArray[17] * 60, // min to sec
        localDailyRainAccumulation: obsArray[18],
        rainAccumulatedFinal: obsArray[19],
        localDailyRainAccumulationFinal: obsArray[20],
        precipitationAnalysisType: obsArray[21]
      };
      
      Object.assign(data, parsedObs);
      delete data.obs;
    }
    
    // Send to SignalK
    const delta = createSignalKDelta(
      'environment.outside.tempest.observations',
      data,
      'mqtt-weatherflow-ws'
    );
    app.handleMessage(plugin.id, delta);
  }

  // Process rapid wind observations
  function processRapidWind(data, options) {
    if (!data.ob) return;
    
    const [timeEpoch, windSpeed, windDirection] = data.ob;
    const windData = {
      timeEpoch,
      windSpeed,
      windDirection: windDirection * (Math.PI / 180), // Convert to radians
      utcDate: new Date(timeEpoch * 1000).toISOString()
    };
    
    const delta = createSignalKDelta(
      'environment.outside.rapidWind',
      windData,
      'mqtt-weatherflow-udp'
    );
    app.handleMessage(plugin.id, delta);
    
    // Calculate wind values if enabled
    if (options.enableWindCalculations && windCalculations) {
      calculateAndPublishWind({
        windSpeed,
        windDirection,
        airTemperature: windCalculations.airTemp
      });
    }
  }

  // Calculate and publish wind values
  function calculateAndPublishWind(windData) {
    if (!windCalculations) return;
    
    try {
      const apparentWind = windCalculations.calculateApparentWind(windData);
      const derivedWind = windCalculations.calculateDerivedWindValues(apparentWind);
      const windDeltas = windCalculations.createWindDeltas(derivedWind);
      
      // Send all wind deltas to SignalK
      windDeltas.forEach(delta => {
        app.handleMessage(plugin.id, delta);
      });
    } catch (error) {
      app.debug('Error calculating wind values:', error);
    }
  }

  // Process Tempest station observations
  function processTempestObservation(data, options) {
    if (!data.obs || !data.obs[0]) return;
    
    const obs = data.obs[0];
    const observationData = {
      timeEpoch: obs[0],
      windLull: obs[1],
      windAvg: obs[2],
      windGust: obs[3],
      windDirection: obs[4] * (Math.PI / 180), // Convert to radians
      windSampleInterval: obs[5],
      stationPressure: obs[6] * 100, // MB to Pa
      airTemperature: obs[7] + 273.15, // °C to K
      relativeHumidity: obs[8],
      illuminance: obs[9],
      uvIndex: obs[10],
      solarRadiation: obs[11],
      rainAccumulated: obs[12],
      precipitationType: obs[13],
      lightningStrikeAvgDistance: obs[14] * 1000, // km to m
      lightningStrikeCount: obs[15],
      battery: obs[16],
      reportInterval: obs[17] * 60, // min to sec
      localDailyRainAccumulation: obs[18],
      rainAccumulatedFinal: obs[19],
      localDailyRainAccumulationFinal: obs[20],
      precipitationAnalysisType: obs[21],
      utcDate: new Date(obs[0] * 1000).toISOString()
    };
    
    const delta = createSignalKDelta(
      'environment.outside.tempest.observations',
      observationData,
      'mqtt-weatherflow-udp'
    );
    app.handleMessage(plugin.id, delta);
    
    // Calculate wind values if enabled
    if (options.enableWindCalculations && windCalculations) {
      calculateAndPublishWind({
        windSpeed: obs[2], // windAvg
        windDirection: obs[4], // windDirection in degrees
        airTemperature: obs[7] + 273.15 // airTemperature in K
      });
    }
  }

  // Process Air station observations
  function processAirObservation(data, options) {
    if (!data.obs || !data.obs[0]) return;
    
    const obs = data.obs[0];
    const observationData = {
      timeEpoch: obs[0],
      stationPressure: obs[1] * 100, // MB to Pa
      airTemperature: obs[2] + 273.15, // °C to K
      relativeHumidity: obs[3],
      lightningStrikeCount: obs[4],
      lightningStrikeAvgDistance: obs[5] * 1000, // km to m
      battery: obs[6],
      reportInterval: obs[7] * 60, // min to sec
      utcDate: new Date(obs[0] * 1000).toISOString()
    };
    
    const delta = createSignalKDelta(
      'environment.inside.air.observations',
      observationData,
      'mqtt-weatherflow-udp'
    );
    app.handleMessage(plugin.id, delta);
  }

  // Process rain events
  function processRainEvent(data, options) {
    if (!data.evt) return;
    
    const [timeEpoch] = data.evt;
    const rainData = {
      timeEpoch,
      utcDate: new Date(timeEpoch * 1000).toISOString()
    };
    
    const delta = createSignalKDelta(
      'environment.outside.rain.observations',
      rainData,
      'mqtt-weatherflow-udp'
    );
    app.handleMessage(plugin.id, delta);
  }

  // Process lightning events
  function processLightningEvent(data, options) {
    if (!data.evt) return;
    
    const [timeEpoch, distance, energy] = data.evt;
    const lightningData = {
      timeEpoch,
      distance: distance * 1000, // km to m
      energy,
      utcDate: new Date(timeEpoch * 1000).toISOString()
    };
    
    const delta = createSignalKDelta(
      'environment.outside.lightning.observations',
      lightningData,
      'mqtt-weatherflow-udp'
    );
    app.handleMessage(plugin.id, delta);
  }

  // Process forecast data
  function processForecastData(data) {
    // Process current conditions
    if (data.current_conditions) {
      const delta = createSignalKDelta(
        'environment.outside.tempest.observations',
        data.current_conditions,
        'mqtt-weatherflow-api'
      );
      app.handleMessage(plugin.id, delta);
    }
    
    // Process hourly forecast (first 72 hours)
    if (data.forecast && data.forecast.hourly) {
      data.forecast.hourly.slice(0, 72).forEach((forecast, index) => {
        // Convert units
        if (forecast.air_temperature !== undefined) {
          forecast.air_temperature += 273.15; // °C to K
        }
        if (forecast.feels_like !== undefined) {
          forecast.feels_like += 273.15; // °C to K
        }
        if (forecast.sea_level_pressure !== undefined) {
          forecast.sea_level_pressure *= 100; // MB to Pa
        }
        if (forecast.station_pressure !== undefined) {
          forecast.station_pressure *= 100; // MB to Pa
        }
        if (forecast.wind_direction !== undefined) {
          forecast.wind_direction *= (Math.PI / 180); // degrees to radians
        }
        
        // Add datetime
        if (forecast.time) {
          forecast.datetime = new Date(forecast.time * 1000).toISOString();
        }
        
        const delta = createSignalKDelta(
          `environment.outside.tempest.forecast.hourly.${index}`,
          forecast,
          'mqtt-weatherflow-api'
        );
        app.handleMessage(plugin.id, delta);
      });
    }
    
    // Process daily forecast (first 10 days)
    if (data.forecast && data.forecast.daily) {
      data.forecast.daily.slice(0, 10).forEach((forecast, index) => {
        // Convert units
        if (forecast.air_temp_high !== undefined) {
          forecast.air_temp_high += 273.15; // °C to K
        }
        if (forecast.air_temp_low !== undefined) {
          forecast.air_temp_low += 273.15; // °C to K
        }
        
        // Add datetime
        if (forecast.day_start_local) {
          forecast.day_start_local_iso = new Date(forecast.day_start_local * 1000).toISOString();
        }
        if (forecast.sunrise) {
          forecast.sunrise_iso = new Date(forecast.sunrise * 1000).toISOString();
        }
        if (forecast.sunset) {
          forecast.sunset_iso = new Date(forecast.sunset * 1000).toISOString();
        }
        
        const delta = createSignalKDelta(
          `environment.outside.tempest.forecast.daily.${index}`,
          forecast,
          'mqtt-weatherflow-api'
        );
        app.handleMessage(plugin.id, delta);
      });
    }
  }

  // Create SignalK delta message
  function createSignalKDelta(path, value, source) {
    const timestamp = new Date().toISOString();
    
    return {
      context: 'vessels.self',
      updates: [{
        $source: source,
        timestamp: timestamp,
        values: [{
          path: path,
          value: value
        }]
      }]
    };
  }

  return plugin;
};