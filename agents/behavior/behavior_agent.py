import logging

from agents.behavior.behavior_rule_engine import BehaviorRuleEngine
from agents.behavior.severity_classifier import SeverityClassifier
from agents.behavior.baseline_manager import BaselineManager
from agents.behavior.deviation_analyzer import DeviationAnalyzer
from agents.behavior.drift_detector import DriftDetector
from agents.behavior.pattern_detector import PatternDetector
from agents.behavior.behavior_score_calculator import BehaviorScoreCalculator
from agents.behavior.confidence_calculator import ConfidenceCalculator
from agents.behavior.behavior_observation_builder import BehaviorObservationBuilder

logger = logging.getLogger(__name__)


class BehaviorAgent:

    # =====================================================
    # INITIALIZATION
    # =====================================================

    def __init__(self, config_file):

        logger.info("Initializing Behavior Agent...")

        self.rule_engine = BehaviorRuleEngine(config_file)

        self.severity_classifier = SeverityClassifier(self.rule_engine)

        self.baseline_manager = BaselineManager(self.rule_engine)

        self.deviation_analyzer = DeviationAnalyzer(
            self.rule_engine,
            self.severity_classifier
        )

        self.drift_detector = DriftDetector(
            self.rule_engine,
            self.baseline_manager
        )

        self.pattern_detector = PatternDetector(self.rule_engine)

        self.behavior_score_calculator = BehaviorScoreCalculator(
            self.rule_engine,
            self.severity_classifier
        )

        self.confidence_calculator = ConfidenceCalculator(self.rule_engine)

        self.behavior_observation_builder = BehaviorObservationBuilder()

        self.total_observations = 0
        self.total_behaviors = 0

        logger.info("Behavior Agent Ready.")

    def _get_severity_order(self):

        return self.rule_engine.get_severity_order()

    def _get_behavior_severity(self, deviation_analysis):

        severity_order = self._get_severity_order()

        if not deviation_analysis:

            return severity_order[0]

        severity_rank = {
            severity: index
            for index, severity in enumerate(severity_order)
        }

        return max(
            (result["severity"] for result in deviation_analysis.values()),
            key=lambda severity: severity_rank.get(severity, -1)
        )

    # =====================================================
    # HEALTH CHECK
    # =====================================================

    def health_check(self):

        logger.info("Running Behavior Agent Health Check...")

        self.rule_engine.health_check()

        self.baseline_manager.health_check()

        logger.info("Severity Classifier Ready.")
        logger.info("Deviation Analyzer Ready.")
        logger.info("Drift Detector Ready.")
        logger.info("Pattern Detector Ready.")
        logger.info("Behavior Score Calculator Ready.")
        logger.info("Confidence Calculator Ready.")
        logger.info("Behavior Observation Builder Ready.")

        logger.info("Behavior Agent Health Check PASSED.")

    # =====================================================
    # ANALYZE SINGLE OBSERVATION
    # =====================================================

    def analyze_observation(self, observation_object):

        entity = observation_object.get("entity", {})

        entity_id = observation_object.get("entity_id")

        deviation_analysis = self.deviation_analyzer.analyze(entity)

        drift_analysis = self.drift_detector.detect(
            entity_id,
            deviation_analysis
        )

        patterns = self.pattern_detector.detect(deviation_analysis)

        behavior_score = self.behavior_score_calculator.calculate(
            deviation_analysis
        )

        severity = self._get_behavior_severity(deviation_analysis)

        confidence = self.confidence_calculator.calculate(
            entity,
            deviation_analysis
        )

        behavior_analysis = {

            "deviation": deviation_analysis,

            "drift": drift_analysis,

            "patterns": patterns,

            "behavior_score": behavior_score,

            "severity": severity,

            "confidence": confidence

        }

        behavior_object = self.behavior_observation_builder.build(

            observation_object,

            behavior_analysis

        )

        # Retrieve and attach persistent historical operational memory context
        try:
            from services.memory_retriever import OperationalMemoryRetriever
            behavior_object["historical_context"] = OperationalMemoryRetriever.retrieve_historical_evidence(behavior_object)
        except Exception as e:
            logger.warning(f"Failed to retrieve operational memory: {e}")
            behavior_object["historical_context"] = {
                "available": False,
                "retrieval_query": "",
                "retrieval_count": 0,
                "matches": []
            }

        # Phase 4: Evidence-based intelligence layer (additive — does not
        # modify any existing behavior field or risk input).
        try:
            from services.phase4_orchestrator import compute_phase4_intelligence
            behavior_object["phase4_intelligence"] = compute_phase4_intelligence(
                behavior_object
            )
        except Exception as e:
            logger.warning(f"Phase 4 intelligence failed gracefully: {e}")
            behavior_object["phase4_intelligence"] = {"available": False}

        return behavior_object

    # =====================================================
    # ANALYZE DATASET
    # =====================================================

    def analyze_dataset(self, observation_objects):

        behavior_objects = []

        for observation_object in observation_objects:

            behavior_objects.append(

                self.analyze_observation(observation_object)

            )

        return behavior_objects

    # =====================================================
    # ANALYZE ALL DATASETS
    # =====================================================

    def analyze_all(self, observation_objects_by_dataset):

        logger.info("Generating Behavior Objects...")

        behavior_objects = {}

        self.total_observations = 0
        self.total_behaviors = 0

        for dataset_name, observations in observation_objects_by_dataset.items():

            logger.info(f"Analyzing Dataset : {dataset_name}")

            dataset_behaviors = self.analyze_dataset(observations)

            behavior_objects[dataset_name] = dataset_behaviors

            self.total_observations += len(observations)

            self.total_behaviors += len(dataset_behaviors)

        logger.info(
            f"Observation Objects Received : {self.total_observations}"
        )

        logger.info(
            f"Behavior Objects Generated : {self.total_behaviors}"
        )

        logger.info("Behavior Agent Completed Successfully.")

        return behavior_objects

    # =====================================================
    # SUMMARY
    # =====================================================

    def summary(self):

        return {
            "observation_objects": self.total_observations,
            "behavior_objects": self.total_behaviors
        }
