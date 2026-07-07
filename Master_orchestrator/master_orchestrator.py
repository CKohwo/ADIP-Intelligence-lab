from intelligence_system.orchestrator.transform_pipeline import run_transform_pipeline
from intelligence_system.orchestrator.features_pipeline import run_all_features_pipelines
from llm_system.orchestrator.runner import run_insight_pipeline

"""
ADIP MASTER ORCHESTRATOR
1. TRANSFORM PIPELINE (RAW -> TRANSFORMED DATA)
2. FEATURE PIPELINE (TRANSFORMED DATA -> FEATURES + TIMESERIES) 
3. LLM INSIGHT
"""

def run_orchestrator ():
    print("ADIP MASTER ORCHESTRATOR STARTED")

    print("Running transform pipeline...")
    transformers_result = run_transform_pipeline()

    print("Running feature pipelines...")
    featured_results = run_all_features_pipelines()

    print("Running LLM insights...")
    llm_insights = run_insight_pipeline()

    print("ADIP MASTER ORCHESTRATOR COMPLETED")

    return {
        "Transformers": transformers_result,
        "Feature_Timeseries": featured_results,
        "LLM_Insights": llm_insights
    }


if __name__ == "__main__":
    run_orchestrator()

