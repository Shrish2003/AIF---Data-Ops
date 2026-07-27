import logging

from agents.recommendation.recommendation_rule_engine import (
    RecommendationRuleEngine
)
from agents.recommendation.recommendation_engine import (
    RecommendationEngine
)
from agents.recommendation.knowledge_retriever import KnowledgeRetriever
from agents.recommendation.prompt_builder import PromptBuilder
from agents.recommendation.recommendation_validator import (
    RecommendationValidator
)
from agents.recommendation.recommendation_object_builder import (
    RecommendationObjectBuilder
)
from agents.multi_llm_selection.multi_llm_selection_agent import (
    MultiLLMSelectionAgent
)
from agents.recommendation.evaluation.evaluation_service import (
    RecommendationEvaluationService
)

logger = logging.getLogger(__name__)


class RecommendationAgent:
    """
    ==========================================================
    Recommendation Agent

    Responsibility:
        Generate one remediation recommendation per entity by
        reasoning over the Behavior Object, Risk Object,
        Integrity Object, and Operational Entity — using the
        deterministic Recommendation Engine and retrieved
        operational knowledge to always produce a rule-based
        recommendation first, then handing off to the
        Multi-LLM Selection Agent to decide whether — and with
        which model — that recommendation should be enhanced.
        This agent does NOT calculate behavior, risk, priority,
        or category; those are already produced by earlier
        agents / the Recommendation Engine. Every entity
        receives a recommendation, including LOW priority ones
        — nothing is ever skipped, and no LLM outage of any
        kind ever prevents a recommendation from being produced
        (see the deterministic fallback path).

    Input:
        Behavior Object, Risk Object, Integrity Object,
        Operational Entity (optional)

    Output:
        Recommendation Object

    DOES NOT
    --------
    - Detect anomalies
    - Calculate Behavior Score or Risk Score
    - Select or call an LLM directly (that is the Multi-LLM
      Selection Agent's responsibility)
    - Validate output correctness (that is the Integrity
      Agent's / Recommendation Validator's responsibility)
    ==========================================================
    """

    # =====================================================
    # INITIALIZATION
    # =====================================================

    def __init__(self, config_file="config/recommendation_rules.yaml",
                 prompt_template_file="config/prompt_template.txt"):

        logger.info("Initializing Recommendation Agent...")

        self.rule_engine = RecommendationRuleEngine(config_file)

        self.recommendation_engine = RecommendationEngine(
            self.rule_engine
        )

        self.knowledge_retriever = KnowledgeRetriever(self.rule_engine)

        self.prompt_builder = PromptBuilder(prompt_template_file)

        self.multi_llm_selection_agent = MultiLLMSelectionAgent()

        self.recommendation_validator = RecommendationValidator(
            self.rule_engine
        )

        self.recommendation_object_builder = RecommendationObjectBuilder()

        self.evaluation_service = RecommendationEvaluationService()

        self.total_behaviors = 0
        self.total_recommendations = 0
        self.total_generated_by_llm = 0
        self.total_generated_by_deterministic = 0

        logger.info("Recommendation Agent Ready.")

    # =====================================================
    # HEALTH CHECK
    # =====================================================

    def health_check(self):

        logger.info("Running Recommendation Agent Health Check...")

        self.rule_engine.health_check()

        logger.info("Recommendation Engine Ready.")
        logger.info("Knowledge Retriever Ready.")
        logger.info("Prompt Builder Ready.")

        self.multi_llm_selection_agent.health_check()

        logger.info("Recommendation Validator Ready.")
        logger.info("Recommendation Object Builder Ready.")

        logger.info("Recommendation Agent Health Check PASSED.")

    # =====================================================
    # BUILD RULE-BASED (DETERMINISTIC) RECOMMENDATION
    #
    # Always valid by construction — guarantees a
    # Recommendation Object is produced even with no LLM
    # connectivity at all. This is now built FIRST, before any
    # LLM is ever considered, for every priority.
    # =====================================================

    def _build_rule_recommendation(self, context):

        fallback_config = self.rule_engine.get_fallback_config()

        recommendation_by_priority = fallback_config.get(
            "recommendation_by_priority", {}
        )

        priority = context.get("priority")

        recommendation_text = recommendation_by_priority.get(
            priority,
            "Review this entity; no specific rule-based recommendation "
            "configured for this priority."
        )

        return {

            "priority": priority,

            "recommendation": recommendation_text,

            "reason": fallback_config.get(
                "reason",
                "Generated by the deterministic Recommendation Engine."
            ),

            "expected_impact": context.get("expected_impact"),

            "estimated_recovery_time": context.get(
                "estimated_recovery_time"
            ),

            "confidence": fallback_config.get("confidence", 0.3),

            "automation_possible": context.get("automation_possible"),

            "human_approval_required": context.get(
                "human_approval_required"
            )

        }

    # =====================================================
    # GENERATE SINGLE RECOMMENDATION
    #
    # Flow:
    #   Recommendation Engine (deterministic context + priority
    #   + impact)
    #     -> Knowledge Retriever
    #     -> Rule-Based Recommendation (always built)
    #     -> Multi-LLM Selection Agent (LOW/MEDIUM returns the
    #        rule recommendation immediately; HIGH/CRITICAL
    #        tries Gemini -> Grok -> Ollama -> deterministic)
    #     -> Recommendation Validator (final safety net)
    #     -> Recommendation Object Builder
    # =====================================================

    def generate_recommendation(
        self,
        behavior_object,
        risk_object,
        integrity_object,
        operational_entity=None
    ):

        context = self.recommendation_engine.analyze(

            behavior_object,

            risk_object,

            integrity_object,

            operational_entity

        )

        retrieved_knowledge = self.knowledge_retriever.retrieve(context)

        rule_recommendation = self._build_rule_recommendation(context)

        # The Prompt Builder's signature and template file are
        # unchanged; the already-decided rule recommendation is
        # passed through as extra "knowledge" fields so the
        # prompt template can tell the LLM what has already
        # been determined and must not be changed.
        prompt_fields = dict(retrieved_knowledge)

        prompt_fields["base_recommendation"] = rule_recommendation.get(
            "recommendation"
        )

        prompt_fields["base_reason"] = rule_recommendation.get("reason")

        prompt_fields["base_confidence"] = rule_recommendation.get(
            "confidence"
        )

        prompt = self.prompt_builder.build(context, prompt_fields)

        candidate = self.multi_llm_selection_agent.select(

            prompt,

            rule_recommendation,

            context.get("priority"),

            entity_id=context.get("entity_id")

        )

        is_valid, reasons = self.recommendation_validator.validate(
            candidate
        )

        if not is_valid:

            logger.warning(
                f"Recommendation for entity {context.get('entity_id')} "
                f"failed final validation ({reasons}). Reverting to the "
                "rule-based recommendation."
            )

            candidate = dict(rule_recommendation)

            candidate["recommendation_source"] = "deterministic"

            candidate["selected_model"] = None

        if candidate.get("recommendation_source") == "llm":

            self.total_generated_by_llm += 1

        else:

            self.total_generated_by_deterministic += 1

        recommendation_object = self.recommendation_object_builder.build(

            context.get("entity_id"),

            candidate

        )

        print("Recommendation Generated Successfully")
        print("Evaluation Started")
        
        evaluated_recommendation_object = self.evaluation_service.evaluate(
            recommendation_object,
            context
        )
        
        print("Evaluation Completed")
        
        evaluation = evaluated_recommendation_object.get("evaluation", {})
        status = evaluation.get("status")
        metric_name = "factual_correctness"
        metrics = evaluation.get("metrics", {})
        score = metrics.get(metric_name, "N/A")
        
        print(f"Metric : {metric_name}")
        print(f"Score : {score}")
        print(f"Status : {status}")

        return evaluated_recommendation_object

    # =====================================================
    # GENERATE FOR ONE DATASET
    #
    # Matches Behavior Objects to their Risk and Integrity
    # Objects by entity_id.
    # =====================================================

    def generate_dataset(
        self,
        operational_entities,
        behavior_objects,
        risk_objects,
        integrity_objects
    ):

        risk_by_entity_id = {

            risk_object.get("entity_id"): risk_object

            for risk_object in risk_objects

        }
        operational_by_entity_id = {

            operational_entity.get("entity_id"): operational_entity

            for operational_entity in operational_entities

        }

        integrity_by_entity_id = {

            integrity_object.get("entity_id"): integrity_object

            for integrity_object in integrity_objects

        }

        recommendation_objects = []

        seen_entity_ids = set()
        deduplicated_behavior_objects = []
        for behavior_object in behavior_objects:
            entity_id = behavior_object.get("entity_id")
            if entity_id not in seen_entity_ids:
                seen_entity_ids.add(entity_id)
                deduplicated_behavior_objects.append(behavior_object)

        for behavior_object in deduplicated_behavior_objects:

            entity_id = behavior_object.get("entity_id")

            recommendation_objects.append(

                self.generate_recommendation(

                    behavior_object,

                    risk_by_entity_id.get(entity_id, {}),

                    integrity_by_entity_id.get(entity_id, {}),

                    operational_by_entity_id.get(entity_id, {})

                )

            )

        return recommendation_objects

    # =====================================================
    # GENERATE FOR ALL DATASETS
    # =====================================================

    def generate_all(
        self,
        operational_entities_by_dataset,
        behavior_objects_by_dataset,
        risk_objects_by_dataset,
        integrity_objects_by_dataset
    ):

        logger.info("Generating Recommendation Objects...")

        recommendation_objects = {}

        self.total_behaviors = 0
        self.total_recommendations = 0
        self.total_generated_by_llm = 0
        self.total_generated_by_deterministic = 0

        for dataset_name, behaviors in behavior_objects_by_dataset.items():

            logger.info(f"Generating Recommendations : {dataset_name}")

            dataset_recommendations = self.generate_dataset(

                operational_entities_by_dataset.get(dataset_name, []),

                behaviors,

                risk_objects_by_dataset.get(dataset_name, []),

                integrity_objects_by_dataset.get(dataset_name, [])

            )

            recommendation_objects[dataset_name] = dataset_recommendations

            self.total_behaviors += len(behaviors)

            self.total_recommendations += len(dataset_recommendations)

        logger.info(
            f"Behavior Objects Received : {self.total_behaviors}"
        )

        logger.info(
            f"Recommendation Objects Generated : "
            f"{self.total_recommendations}"
        )

        logger.info("Recommendation Agent Completed Successfully.")

        return recommendation_objects

    # =====================================================
    # SUMMARY
    # =====================================================

    def summary(self):

        print("\n")
        print("=" * 70)
        print("RECOMMENDATION AGENT SUMMARY")
        print("=" * 70)

        print(f"Behavior Objects            : {self.total_behaviors}")
        print(f"Recommendation Objects       : {self.total_recommendations}")
        print(f"Generated via LLM            : {self.total_generated_by_llm}")
        print(f"Generated via Deterministic  : {self.total_generated_by_deterministic}")

        print("=" * 70)

        self.multi_llm_selection_agent.summary()
