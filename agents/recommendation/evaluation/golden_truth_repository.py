import os
import json
import logging

logger = logging.getLogger(__name__)

class GoldenTruthRepository:
    """
    ==========================================================
    Golden Truth Repository
    
    Responsibility:
        Encapsulate database/file operations for retrieving
        Golden Truth scenarios. Decouples Scenario Resolver
        from the filesystem backend.
        
    Future Expansion:
        Can be extended to support database lookups, caching,
        or data versioning without changing other components.
    ==========================================================
    """

    def __init__(self, dataset_path="data/evaluation/golden_truth_recommendations.json"):
        self.dataset_path = dataset_path
        self._cached_scenarios = None
        logger.info(f"Golden Truth Repository initialized with path: {self.dataset_path}")

    def load(self):
        """
        Load and parse the JSON dataset. Caches it in memory to prevent
        redundant disk reads.
        """
        if self._cached_scenarios is not None:
            return self._cached_scenarios

        if not os.path.exists(self.dataset_path):
            logger.error(f"Golden Truth dataset file not found at: {self.dataset_path}")
            self._cached_scenarios = []
            return self._cached_scenarios

        try:
            with open(self.dataset_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    logger.error("Golden Truth dataset is not in list format.")
                    self._cached_scenarios = []
                else:
                    self._cached_scenarios = data
                    logger.info(f"Successfully loaded {len(data)} scenarios from Golden Truth dataset.")
        except Exception as e:
            logger.error(f"Error loading Golden Truth dataset: {e}")
            self._cached_scenarios = []

        return self._cached_scenarios

    def get_all_scenarios(self):
        """
        Retrieve all loaded scenarios.
        """
        return self.load()
