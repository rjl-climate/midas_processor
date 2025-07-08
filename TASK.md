# REFACTOR Observation enrichment

There is a possible inconsistency or improvement opportunity in the way we create the station registry, capture station operational dates and relate them to changes of id within the operational period.

There is now a file that is available for each dataset that summarises station information. An example for the uk-daily-temperature-obs dataset is here: '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/station-metadata/midas-open_uk-daily-temperature-obs_dv-202507_station-metadata.csv'

This is not currently processed by the station_registry service.

Individual weather capability files (e.g. '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/capability/aberdeenshire/00144_corgarff-castle-lodge/midas-open_uk-daily-temperature-obs_dv-202507_aberdeenshire_00144_corgarff-castle-lodge_capability.csv') contain information about how the id of the station has changed during the period of its operation.

These two pieces of data have to be captured, reconciled, and used when validating observations during csv parsing and recortd processing. This should resolve the current bug in validation.
