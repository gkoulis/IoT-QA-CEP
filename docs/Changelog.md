Changelog
===

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
