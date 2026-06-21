from intelligence_system.orchestrator.transform_pipeline import run_transform_pipeline
from intelligence_system.orchestrator.features_pipeline import run_all_features_pipelines

"""
ADIP MASTER ORCHESTRATOR
1. TRANSFORM PIPELINE (RAW -> TRANSFORMED DATA)
2. FEATURE PIPELINE (TRANSFORMED DATA -> FEATURES + TIMESERIES) 
"""

def run_orchestrator ():
    print("ADIP MASTER ORCHESTRATOR")

    transformers_result = run_transform_pipeline()
    featured_results = run_all_features_pipelines()

    return {
        "Transformers": transformers_result,
        "Feature_Timeseries": featured_results 
    }


if __name__ == "__main__":
    run_orchestrator()

