Changelog
===

## v.1.0.-PROTOTYPE.18 Thursday 28 March 2024

### Event Engine

- Changed alphas in timeliness calculation: naive = 0.8, exponential smoothing = 0.9.
- Improved `ExponentialSmoothingWithLinearTrendOptimization`.
- Using `ExponentialSmoothingWithLinearTrendOptimization` instead of `ExponentialSmoothingWithLinearTrend` for forecasting in `EventFabricationService`. This is 20 to 30 times slower, however, it provides much better forecasts.

### Extensions

- Improved average calculation parameters set ID manipulation.
- Improved presets and base (for convenient customization).
- Fixed bug in accuracy calculation.
- Some other changes and improvements.

## v1.0.0-PROTOTYPE.17 Monday 18 March 2024

### Event Engine

- Introducing new improved version for calculating degree of timeliness and mean timeliness.

## v1.0.0-PROTOTYPE.16 Monday 18 March 2024

### Event Engine

- Integrated `ExponentialSmoothingWithLinearTrend` to `EventFabricationService`.
- Added `ensureCandidateTimeConsistency` method to `EventFabricationService` to ensure distance consistency.

## v1.0.0-PROTOTYPE.15 Monday 18 March 2024

### Event Engine

- Improved (experimental) `TripleExponentialSmoothing` and `TripleExponentialSmoothingOptimization`.
- Introducing (experimental) `ExponentialSmoothingWithLinearTrend` and `ExponentialSmoothingWithLinearTrendOptimization`.
- Moved MSE calculation to `CalculationUtils`.

## v1.0.0-PROTOTYPE.14 Monday 18 March 2024

### Event Engine

- Introducing (experimental) `TripleExponentialSmoothing` and `TripleExponentialSmoothingOptimization`.

## v1.0.0-PROTOTYPE.13 Friday 15 March 2024

trivial commit  

### Event Engine  

- changed paths  

### Extensions  

- Changed paths  

## v1.0.0-PROTOTYPE.12 Thursday 14 March 2024

### Event Engine

- Introducing `IBOPersistenceService`, `IBOPersistenceServiceBaseImpl`, `IBOPersistenceServiceMongoImpl`, and `IBOPersistenceServiceNoOpsImpl` to ensure flexibility regarding persistence back-ends and simulation options.

### Extensions

- Removed `shared`, `server_client`, `helpers`, `fabrication_forecasting`, `fabrication_backcasting`, `sensing_recording`, packages
- Refactors and removals.

### Misc

- Dropped support for Apache Thrift
- Removed `iotvm-extensions-specifications`


## v1.0.0-PROTOTYPE.11 Thursday 14 March 2024

### Event Engine

- Removed `findPastSensorTelemetryMeasurementEventIBO` from `IBOPersistenceServiceImpl`.

## v1.0.0-PROTOTYPE.10 Wednesday 13 March 2024

### Event Engine

- Removed `extensions` package (ExtensionsClientsFactory, FabricationForecastingServiceAdapter, ReconnectiveThriftClient)
- Removed `shared.extensions` package (Apache Thrift objects)
- Removed past events and forecasting fabrication processors from average calculation composite transformation.
- Removed redundant and unnecessary code from average calculation composite transformation.
- Added mapper to map `OutputEvent` to `SensorTelemetryMeasurementEventIBO`.
- Added `EventFabricationMethod` enumeration with the supported event fabrication methods.
- Completed integration of the new `EventFabricationService` to the average calculation composite transformation.
- Removed support for Apache Thrift (pom.xml dependency).
- Removed `FabricationValueMapperWithKey`.

### Extensions

- Improved and refactored complex event evaluation (new keys, new data, percentages, roundings, etc)

## v1.0.0-PROTOTYPE.9 Wednesday 13 March 2024

### Extensions

- Added presets for convenient simulation setup
- Improved simulation (evaluation, examples, presents, constants)

## v1.0.0-PROTOTYPE.8 Wednesday 13 March 2024

### Event Engine

- Experimental integration of new `EventFabricationService` to average calculation composite transformation.  

## v1.0.0-PROTOTYPE.7 Wednesday 13 March 2024

### Event Engine

- Introducing CEP-native `EventFabricationService` for performing event fabrication using naive (implemented) and simple exponential smoothing (not yet implemented) methods.

## v1.0.0-PROTOTYPE.6 Wednesday 13 March 2024

### Event Engine

- Introducing `TimeWindowedTimeSeries` (and test). It is an optimized time-series structure for real-time time-windowed operations. It also contain time-window utilities and missing point management (linear interpolation).

## v1.0.0-PROTOTYPE.5 Tuesday 12 March 2024

### Extensions

- Simulation / Variation / Iteration persistence (complex event and evaluation, i.e., accuracy calculation)

## v1.0.0-PROTOTYPE.4 Tuesday 12 March 2024

### Event Engine

- Removed SensingRecordingServiceAdapter (and calculation based on ground-truth)
- Important changes: simulation (still working on it)

### Extensions

- Important changes: simulation (still working on it)

## v1.0.0-PROTOTYPE.3 Tuesday 12 March 2024

- Event Engine: Added simulation package.
- Event Engine: added general utilities class.
- `iotvm_extensions`: Added functionality to create/persist composite transformations parameters sets. Plus some improvements and changes.

## v1.0.0-PROTOTYPE.2 Monday 11 March 2024

- Added new simulation module in `iotvm_extensions` project.
- `iotvm_extensions` code format
- Added CEP simulation functionality to Event Engine.

## Saturday 04 November 2023

Many changes and improvements.  

## Friday 13 October 2023

### iotvm-eventengine

- Changed Java: from 20 to 19.
- Average Calculation Composite Transformation Parameters are loaded from a JSON file (resource).
- When `forecastingWindowSize` is null, it is initialized with the double duration of `timeWindowSize`. 
- When `futureWindowsLookup` is less than or equal to zero, forecasting is disabled.
- Added more debug strings: simulation name, experiment name.
- Fixed bugs in debug strings construction (simulation and simulation name conflict).
- Changed Kafka configuration path to kafka-streams directory (for Ubuntu).

### iotvm-gateway
- 
- Changed Java: from 20 to 19.

### iotvm-extensions

## Tuesday 10 October 2023

- Synthetic data generator (with errors, up-sampling, etc).

## v1.0.0-PROTOTYPE.1

Features:

- extensions (python, thrift, event engine, gateway)
- fabrication forecasting
- fabrication backcasting
- sensing recordind
- event engine improvements
- gateway improvements
- web app
- bug fixes

# Sunday 30 July 2023

## iotvm-eventengine

- Introducing `FabricationValueMapperWithKey` class for fabrications.
- Refactored `AverageCalculationCompositeTransformationFactory`: improvements, replaced `ValueMapperWithKey` with `FabricationValueMapperWithKey`.
- Changes in business logic of past event fabrication (number of events, priority, best effort to select the most recent past events, etc).
- Changes in business logic of forecasted event fabrication (number of events, priority, best effort to select the accurate forecasted events, etc)..
- Το soft sensing ΔΕΝ ενεργοποιείται όταν ένα χρονικό παράθυρο έχει μηδέν events.  
  Άρα, το soft sensing ενεργοποιείται όταν ένα χρονικό παράθυρο έχει τουλάχιστον ένα real event.   
  Αυτό ισχύει και για past events και για forecasted events.  
- Fixed bug: average calculation was missing from fabrications.
