import logging
from datetime import datetime
from agents.recommendation.evaluation.golden_truth_repository import GoldenTruthRepository
from agents.recommendation.evaluation.scenario_resolver import ScenarioResolver
from agents.recommendation.evaluation.ragas_evaluator import RagasEvaluator, EvaluationBackendUnavailableError

logger = logging.getLogger(__name__)

class RecommendationEvaluationService:
    """
    ==========================================================
    Recommendation Evaluation Service
    
    Responsibility:
        Orchestrates all steps of the recommendation evaluation process.
        Accepts the constructed Recommendation Object and operational context,
        resolves the expected recommendation, triggers RAGAS, builds
        evaluation metadata, and appends it to the final object.
        
    Design:
        - Keeps evaluation logic independent of Recommendation Agent.
        - Gracefully handles unavailability of RAGAS or scenarios.
        - Exposes sync evaluate() and async-ready aevaluate() methods.
    ==========================================================
    """

    def __init__(self, dataset_path="data/evaluation/golden_truth_recommendations.json"):
        self.repository = GoldenTruthRepository(dataset_path)
        self.resolver = ScenarioResolver()
        self.evaluator = RagasEvaluator()
        logger.info("Recommendation Evaluation Service initialized successfully.")

    def evaluate(self, recommendation_object, operational_context):
        """
        Perform synchronous evaluation and append metadata to the recommendation object.
        
        Args:
            recommendation_object (dict): The base built recommendation object.
            operational_context (dict): Operational variables extracted during generation.
            
        Returns:
            dict: The recommendation object with evaluation metadata appended.
        """
        logger.info(f"Evaluation Started for entity: {recommendation_object.get('entity_id')}")
        
        # Initialize default evaluation dictionary
        evaluation_meta = {
            "status": "UNAVAILABLE",
            "engine": "ragas",
            "metrics": {},
            "timestamp": datetime.now().isoformat()
        }

        try:
            # 1. Load Golden Truth Scenarios
            scenarios = self.repository.get_all_scenarios()
            print("Golden Truth Loaded")
            logger.info(f"Repository loaded: {len(scenarios)} scenarios.")
            
            # 2. Resolve Best Scenario Match
            resolved_scenario = self.resolver.resolve(operational_context, scenarios)
            
            if resolved_scenario is None:
                logger.warning(
                    f"No matching Golden Truth scenario found for entity {recommendation_object.get('entity_id')}. "
                    "Evaluation status set to UNAVAILABLE."
                )
                evaluation_meta["status"] = "UNAVAILABLE"
            else:
                print("Scenario Resolved")
                logger.info(f"Scenario resolved: {resolved_scenario.get('scenario_name')}")
                expected_rec = resolved_scenario.get("expected_recommendation")
                generated_rec = recommendation_object.get("recommendation")
                
                logger.info(f"Generated recommendation: {generated_rec}")
                logger.info(f"Expected recommendation: {expected_rec}")
                
                if not expected_rec or not generated_rec:
                    logger.warning("Missing recommendation content. Cannot perform evaluation.")
                    evaluation_meta["status"] = "UNAVAILABLE"
                else:
                    # 3. Perform RAGAS Evaluation
                    try:
                        print("RAGAS Evaluation Started")
                        score = self.evaluator.evaluate_correctness(generated_rec, expected_rec)
                        evaluation_meta["status"] = "COMPLETED"
                        evaluation_meta["metrics"]["factual_correctness"] = score
                    except EvaluationBackendUnavailableError as e:
                        logger.warning(f"RAGAS evaluation backend is not available: {e}. Setting status to UNAVAILABLE.")
                        import traceback
                        traceback.print_exc()
                        evaluation_meta["status"] = "UNAVAILABLE"
                    except Exception as e:
                        logger.error(f"Unexpected error during evaluation scoring: {e}")
                        import traceback
                        traceback.print_exc()
                        evaluation_meta["status"] = "UNAVAILABLE"
                        
        except Exception as e:
            logger.error(f"Failed to orchestrate evaluation: {e}")
            import traceback
            traceback.print_exc()
            evaluation_meta["status"] = "UNAVAILABLE"

        logger.info(f"Evaluation Completed for entity: {recommendation_object.get('entity_id')}")

        # Decouple: Do not modify base builder. Append metadata directly to object.
        final_object = dict(recommendation_object)
        final_object["evaluation"] = evaluation_meta
        return final_object

    async def aevaluate(self, recommendation_object, operational_context):
        """
        Asynchronous wrapper to prepare the system for async execution.
        Currently delegates synchronously but allows non-blocking execution in the future.
        """
        # Run in executor to simulate asynchronous execution
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, 
            self.evaluate, 
            recommendation_object, 
            operational_context
        )
