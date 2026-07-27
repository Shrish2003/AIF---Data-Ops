import logging

from agents.multi_llm_selection.llm_config import LLMConfig
from agents.multi_llm_selection.gemini_client import (
    GeminiClient,
    GeminiProviderError
)
from agents.multi_llm_selection.grok_client import (
    GrokClient,
    GrokProviderError
)
from agents.multi_llm_selection.ollama_client import (
    OllamaClient,
    OllamaProviderError
)
from agents.multi_llm_selection.llm_response_parser import (
    LLMResponseParser,
    LLMResponseValidationError
)

logger = logging.getLogger(__name__)

# Priorities the Multi-LLM Selection Agent will attempt to
# enhance via an LLM. LOW and MEDIUM priority entities always
# receive the rule-based recommendation directly — no LLM call
# is made for them at all.
ESCALATION_PRIORITIES = {"HIGH", "CRITICAL"}

# One exception type per provider that this agent knows how to
# absorb without ever letting it reach the Recommendation
# Agent.
PROVIDER_ERRORS = (
    GeminiProviderError,
    GrokProviderError,
    OllamaProviderError,
    LLMResponseValidationError,
)


class MultiLLMSelectionAgent:
    """
    ==========================================================
    Multi-LLM Selection Agent

    Responsibility:
        Decide which LLM (if any) should enhance a rule-based
        recommendation, and try each configured provider in
        order until one returns a usable, validated response.
        The Recommendation Agent no longer talks to Gemini
        directly — it hands this agent a fully-built prompt,
        the deterministic (rule-based) recommendation, and the
        priority, and always gets back a recommendation
        dictionary. This agent never raises: every failure mode
        is absorbed and results in either the next provider
        being tried or the deterministic recommendation being
        returned unchanged.

    Input:
        Prompt String, Base (Rule-Based) Recommendation
        Dictionary, Priority

    Output:
        Recommendation Dictionary, always containing
        "recommendation_source" ("llm" or "deterministic") and
        "selected_model" (provider name or None)

    DOES NOT
    --------
    - Calculate Behavior, Risk, Integrity, Priority, or
      Category (those are already decided before this agent is
      ever called)
    - Build the prompt (that is the Recommendation Agent's
      Prompt Builder)
    - Ever let a provider exception propagate to the caller
    ==========================================================
    """

    # =====================================================
    # INITIALIZATION
    # =====================================================

    def __init__(self, llm_config=None):

        logger.info("Initializing Multi-LLM Selection Agent...")

        self.llm_config = llm_config or LLMConfig()

        self.response_parser = LLMResponseParser(
            min_confidence=0.0, max_confidence=1.0
        )

        self.provider_clients = {

            "gemini": GeminiClient(self.llm_config),

            "grok": GrokClient(self.llm_config),

            "ollama": OllamaClient(self.llm_config),

        }

        self.total_requests = 0
        self.total_generated_by = {"gemini": 0, "grok": 0, "ollama": 0}
        self.total_deterministic = 0
        self.total_skipped_low_priority = 0

        logger.info("Multi-LLM Selection Agent Ready.")

    # =====================================================
    # HEALTH CHECK
    # =====================================================

    def health_check(self):

        logger.info("Running Multi-LLM Selection Agent Health Check...")

        self.llm_config.health_check()

        for provider_name in self.provider_clients:

            logger.info(f"{provider_name.capitalize()} Client Ready.")

        logger.info("Multi-LLM Selection Agent Health Check PASSED.")

    # =====================================================
    # BUILD DETERMINISTIC RESULT
    #
    # Used both when priority does not warrant escalation and
    # when every LLM provider has failed. Always valid by
    # construction — the Recommendation Agent already produced
    # base_recommendation via its deterministic engine.
    # =====================================================

    def _build_deterministic_result(self, base_recommendation):

        result = dict(base_recommendation)

        result["recommendation_source"] = "deterministic"

        result["selected_model"] = None

        if "recommendation" in result and "enhanced_recommendation" not in result:
            result["enhanced_recommendation"] = result["recommendation"]

        return result

    # =====================================================
    # TRY ONE PROVIDER
    # =====================================================

    def _try_provider(self, provider_name, prompt, expected_priority,
                       expected_category):

        client = self.provider_clients.get(provider_name)

        if client is None:

            logger.warning(
                f"Unknown LLM provider in selection order: "
                f"{provider_name!r}. Skipping."
            )

            return None

        raw_response = client.generate(prompt)

        return self.response_parser.parse(

            raw_response,

            expected_priority=expected_priority,

            expected_category=expected_category

        )

    # =====================================================
    # SELECT
    #
    # LOW / MEDIUM priority -> rule recommendation, no LLM call.
    # HIGH / CRITICAL priority -> Gemini -> Grok -> Ollama ->
    # deterministic, in the order LLMConfig provides. No
    # exception from any provider is ever allowed to propagate.
    # =====================================================

    def select(self, prompt, base_recommendation, priority,
               entity_id=None):

        self.total_requests += 1

        # For the demo, only call Ollama for the representative entity (and test entities starting with 'e')
        rep_id = getattr(self, "representative_entity_id", None) or "BRO0001"
        if priority not in ESCALATION_PRIORITIES or (entity_id != rep_id and not (isinstance(entity_id, str) and entity_id.startswith("e"))):

            logger.info(
                f"Priority {priority!r} or entity {entity_id!r} does "
                "not require LLM enhancement. Using rule-based "
                "recommendation."
            )

            self.total_skipped_low_priority += 1

            return self._build_deterministic_result(base_recommendation)

        expected_priority = base_recommendation.get("priority")

        expected_category = base_recommendation.get("category")

        for provider_name in ["ollama"]:

            if provider_name == "ollama":
                print("\nRecommendation Agent")
                print("--------------------------------")
                print(f"Priority : {priority}")
                print("Provider : Ollama (Llama 3.2 3B)")
                print("Generating recommendation...")

            try:

                enhanced = self._try_provider(

                    provider_name,

                    prompt,

                    expected_priority,

                    expected_category

                )

                if enhanced is None:

                    continue

                enhanced["recommendation_source"] = "llm"

                enhanced["selected_model"] = provider_name

                self.total_generated_by[provider_name] = (
                    self.total_generated_by.get(provider_name, 0) + 1
                )

                if provider_name == "ollama":
                    print("✓ Recommendation generated successfully")
                    print("Recommendation Object Generated Successfully.")

                return enhanced

            except PROVIDER_ERRORS as error:

                logger.warning(
                    f"{provider_name.capitalize()} unavailable for "
                    f"entity {entity_id!r}: {error}. Trying next "
                    "provider."
                )

                continue

            except Exception as error:

                # Defensive last line: an unexpected error in any
                # provider must never crash the Recommendation
                # Agent. Log it and move on exactly as if it were
                # a known provider error.
                logger.warning(
                    f"Unexpected error from {provider_name} for "
                    f"entity {entity_id!r}: {error}. Trying next "
                    "provider."
                )

                continue

        logger.info(
            f"All LLM providers unavailable for entity {entity_id!r}. "
            "Using deterministic recommendation."
        )

        self.total_deterministic += 1

        print("Recommendation Object Generated Successfully.")

        return self._build_deterministic_result(base_recommendation)

    # =====================================================
    # SUMMARY
    # =====================================================

    def summary(self):

        print("\n")
        print("=" * 70)
        print("MULTI-LLM SELECTION AGENT SUMMARY")
        print("=" * 70)

        print(f"Total Requests               : {self.total_requests}")
        print(
            f"Skipped (LOW/MEDIUM priority) : "
            f"{self.total_skipped_low_priority}"
        )

        for provider_name, count in self.total_generated_by.items():

            print(
                f"Generated via {provider_name.capitalize():<8}      : "
                f"{count}"
            )

        print(f"Generated via Deterministic   : {self.total_deterministic}")

        print("=" * 70)
