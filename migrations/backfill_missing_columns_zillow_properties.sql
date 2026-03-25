-- Backfill: populate new columns from raw_json for all existing rows in
-- zillow_properties.  Safe to re-run — NULL values are left as NULL when
-- the key is absent in raw_json.
-- Run AFTER add_missing_columns_zillow_properties.sql.

UPDATE zillow_properties
SET
    -- Top-level scalar fields
    available_from          = raw_json->>'availableFrom',
    building_id             = (raw_json->>'buildingId')::BIGINT,
    contingent_listing_type = raw_json->>'contingentListingType',
    lease_term              = raw_json->>'leaseTerm',

    -- Top-level JSONB fields
    fees               = raw_json->'fees',
    open_house_schedule = raw_json->'openHouseSchedule',

    -- resoData scalar fields
    reso_above_grade_finished_area      = raw_json->'resoData'->>'aboveGradeFinishedArea',
    reso_additional_parcels_description = raw_json->'resoData'->>'additionalParcelsDescription',
    reso_association_fee                = raw_json->'resoData'->>'associationFee',
    reso_availability_date              = CASE
                                            WHEN raw_json->'resoData'->>'availabilityDate' IS NOT NULL
                                            THEN to_timestamp((raw_json->'resoData'->>'availabilityDate')::BIGINT / 1000.0)
                                          END,
    reso_bathrooms_one_quarter          = (raw_json->'resoData'->>'bathroomsOneQuarter')::INTEGER,
    reso_bathrooms_three_quarter        = (raw_json->'resoData'->>'bathroomsThreeQuarter')::INTEGER,
    reso_builder_model                  = raw_json->'resoData'->>'builderModel',
    reso_building_area_source           = raw_json->'resoData'->>'buildingAreaSource',
    reso_carport_parking_capacity       = (raw_json->'resoData'->>'carportParkingCapacity')::INTEGER,
    reso_common_walls                   = raw_json->'resoData'->>'commonWalls',
    reso_cumulative_days_on_market      = raw_json->'resoData'->>'cumulativeDaysOnMarket',
    reso_fencing                        = raw_json->'resoData'->>'fencing',
    reso_has_association                = (raw_json->'resoData'->>'hasAssociation')::BOOLEAN,
    reso_has_carport                    = (raw_json->'resoData'->>'hasCarport')::BOOLEAN,
    reso_has_pets_allowed               = (raw_json->'resoData'->>'hasPetsAllowed')::BOOLEAN,
    reso_has_waterfront_view            = (raw_json->'resoData'->>'hasWaterfrontView')::BOOLEAN,
    reso_hoa_fee                        = raw_json->'resoData'->>'hoaFee',
    reso_hoa_fee_total                  = raw_json->'resoData'->>'hoaFeeTotal',
    reso_irrigation_water_rights_yn     = (raw_json->'resoData'->>'irrigationWaterRightsYN')::BOOLEAN,
    reso_lease_term                     = raw_json->'resoData'->>'leaseTerm',
    reso_levels                         = raw_json->'resoData'->>'levels',
    reso_list_aor                       = raw_json->'resoData'->>'listAOR',
    reso_living_area_range_units        = raw_json->'resoData'->>'livingAreaRangeUnits',
    reso_main_level_bathrooms           = (raw_json->'resoData'->>'mainLevelBathrooms')::INTEGER,
    reso_main_level_bedrooms            = (raw_json->'resoData'->>'mainLevelBedrooms')::INTEGER,
    reso_property_condition             = raw_json->'resoData'->>'propertyCondition',
    reso_stories_total                  = (raw_json->'resoData'->>'storiesTotal')::INTEGER,
    reso_structure_type                 = raw_json->'resoData'->>'structureType',
    reso_virtual_tour                   = raw_json->'resoData'->>'virtualTour',
    reso_water_view                     = raw_json->'resoData'->>'waterView',
    reso_water_view_yn                  = (raw_json->'resoData'->>'waterViewYN')::BOOLEAN,
    reso_year_built                     = (raw_json->'resoData'->>'yearBuilt')::INTEGER,
    reso_year_built_effective           = (raw_json->'resoData'->>'yearBuiltEffective')::INTEGER,
    reso_zoning                         = raw_json->'resoData'->>'zoning',
    reso_zoning_description             = raw_json->'resoData'->>'zoningDescription',

    -- resoData JSONB container fields
    reso_associations                     = raw_json->'resoData'->'associations',
    reso_door_features                    = raw_json->'resoData'->'doorFeatures',
    reso_green_building_verification_type = raw_json->'resoData'->'greenBuildingVerificationType',
    reso_green_energy_efficient           = raw_json->'resoData'->'greenEnergyEfficient',
    reso_green_energy_generation          = raw_json->'resoData'->'greenEnergyGeneration',
    reso_green_water_conservation         = raw_json->'resoData'->'greenWaterConservation',
    reso_horse_amenities                  = raw_json->'resoData'->'horseAmenities',
    reso_media                            = raw_json->'resoData'->'media',
    reso_other_equipment                  = raw_json->'resoData'->'otherEquipment',
    reso_other_facts                      = raw_json->'resoData'->'otherFacts',
    reso_other_parking                    = raw_json->'resoData'->'otherParking',
    reso_other_structures                 = raw_json->'resoData'->'otherStructures',
    reso_pool_features                    = raw_json->'resoData'->'poolFeatures',
    reso_road_surface_type                = raw_json->'resoData'->'roadSurfaceType',
    reso_spa_features                     = raw_json->'resoData'->'spaFeatures',
    reso_utilities                        = raw_json->'resoData'->'utilities',
    reso_view                             = raw_json->'resoData'->'view',
    reso_waterfront_features              = raw_json->'resoData'->'waterfrontFeatures',
    reso_window_features                  = raw_json->'resoData'->'windowFeatures'

WHERE raw_json IS NOT NULL;
