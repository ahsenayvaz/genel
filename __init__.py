from csv_to_dgh.pipeline_settings import load_setting

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import fab_csv_import_assets, station_csv_import_assets
from . import fall_csv_import_assets, fall_csv_import_assets_alpha
from . import loinc_csv_import_assets
from . import ik_csv_import_assets
from . import ucum_csv_import_assets
from . import lab_valuequality_csv_import_assets
from .CAS import import_cas_bezvo_csv_assets, add_substance_cas_ocas_code_assets, add_substance_name_assets, add_substance_assets, medikation_cas_overwrite_assets
from . import atc_csv_import_assets

# Load all assets from the specified module
assets = load_assets_from_modules([
    fall_csv_import_assets,fall_csv_import_assets_alpha,loinc_csv_import_assets,ik_csv_import_assets,
    ucum_csv_import_assets,fab_csv_import_assets,station_csv_import_assets,lab_valuequality_csv_import_assets,
    import_cas_bezvo_csv_assets, add_substance_cas_ocas_code_assets, add_substance_name_assets, add_substance_assets,
    medikation_cas_overwrite_assets, atc_csv_import_assets
])

# ----------------------------------------------------------------------------------------------------------------------
# Job Definition: executes only a subset of the loaded assets (materializes the assets)
# ----------------------------------------------------------------------------------------------------------------------
# (1) Fall CSV Import job
fall_csv_import = define_asset_job(
    "FallCSVImport",
    # Run only the selected assets
    selection=["fall_csv_import","versichertennummer_cleaning","versichertennummer_period"]
)
# Schedule for the job
fall_csv_import_schedule_command = load_setting("FALL_CSV_IMPORT_CRON_SCHEDULE", "value")
if fall_csv_import_schedule_command is None:
    fall_csv_import_schedule_command = "0 0 * * 2"  # minute 0, hour 0, Tuesday (2)
fall_csv_import_schedule = ScheduleDefinition(
    job=fall_csv_import,
    cron_schedule=fall_csv_import_schedule_command,
    execution_timezone="Europe/Berlin"
)

# (1) alpha Fall CSV Import job
fall_csv_import_alpha = define_asset_job(
    "FallCSVImport_alpha",
    # Run only the selected assets
    selection=["fall_csv_import_alpha","add_patient_id","versichertennummer_cleaning_alpha","versichertennummer_period_alpha"]
)

# (2) Loinc CSV Import job
loinc_csv_import = define_asset_job(
    "LoincCSVImport",
    # Run only the selected assets
    selection=["loinc_csv_import","labor_method_use"]
)
# loinc_csv_import_schedule = ScheduleDefinition(
#     job=loinc_csv_import,
#     cron_schedule="0 0 * * 1"  # minute 0, hour 0, Monday (1)
# )

# (3) IK CSV Import job
ik_csv_import = define_asset_job(
    "IKCSVImport",
    # Run only the selected assets
    selection=["ik_csv_import"]
)

# (4) UCUM CSV Import job
ucum_csv_import = define_asset_job(
    "UCUMCSVImport",
    selection=["check_source_to_ucum_mapping","ucum_labor_csv_import","ucum_medikation_csv_import"]
)
# ucum_csv_import_schedule = ScheduleDefinition(
#     job=ucum_csv_import,
#     cron_schedule = "0 0 * * 2" , # minute 0, hour 0, Tuesday (2)
#     execution_timezone="Europe/Berlin"
# )

# (4) FAB CSV Import job
fab_csv_import = define_asset_job(
    "FABCSVImport",
    selection=["check_source_to_fab_mapping","fab_csv_import"]
)
# fab_csv_import_schedule = ScheduleDefinition(
#     job=fab_csv_import,
#     cron_schedule = "0 0 * * 2",  # minute 0, hour 0, Tuesday (2)
#     execution_timezone="Europe/Berlin"
# )

# (5) SCT CSV Import job
lab_sct_csv_import = define_asset_job(
    "LabSCTCSVImport",
    selection=["check_source_to_quality_messwert","messwert_qualitativ_csv_import"]
)
# lab_sct_csv_import_schedule = ScheduleDefinition(
#     job=lab_sct_csv_import,
#     cron_schedule = "0 0 * * 2",  # minute 0, hour 0, Tuesday (2)
#     execution_timezone="Europe/Berlin"
# )

# (6) CAS CSV Import job
medication_cas_csv_import = define_asset_job(
    "MedCASCSVImport",
    selection=["medication_ingredient_cas_import_bezvo_csv",
               "medication_ingredient_cas_code_addition",
               "medication_ingredient_name_addition",
               "medication_ingredient_substance_import",
               "medikation_cas_overwrite_import"
               ]
)
# medication_cas_csv_import_schedule = ScheduleDefinition(
#     job=medication_cas_csv_import,
#     cron_schedule = "0 0 * * 2",  # minute 0, hour 0, Tuesday (2)
#     execution_timezone="Europe/Berlin"
# )

# (7) ATC CSV Import job
atc_csv_import = define_asset_job(
    "ATCCSVImport",
    selection=["atc_csv_import"]
)
# atc_csv_import_schedule = ScheduleDefinition(
#     job=atc_csv_import,
#     cron_schedule = "0 0 * * 2",  # minute 0, hour 0, Tuesday (2)
#     execution_timezone="Europe/Berlin"
# )

# (8) Station CSV Import job
station_csv_import = define_asset_job(
    "StationCSVImport",
    selection=["station_csv_import"]
)
# station_csv_import_schedule = ScheduleDefinition(
#     job=station_csv_import,
#     cron_schedule = "0 0 * * 2",  # minute 0, hour 0, Tuesday (2)
#     execution_timezone="Europe/Berlin"
# )

# ----------------------------------------------------------------------------------------------------------------------
# Dagster Definitions: register all assets, plus the selected jobs & schedules
# ----------------------------------------------------------------------------------------------------------------------
# Register assets, job, and schedule with Dagster
defs = Definitions(
    assets=assets,
    jobs=[fall_csv_import,fall_csv_import_alpha,loinc_csv_import,ik_csv_import, ucum_csv_import,fab_csv_import,
          lab_sct_csv_import,medication_cas_csv_import,atc_csv_import,station_csv_import],
    schedules=[fall_csv_import_schedule],
)

