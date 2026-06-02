"""
ECG Platform — Graph-Grounded Intelligence Agent

A LangGraph agent that uses the ECG graph as its structural backbone:
  - GRAPH does: relationship traversal, path finding, entity resolution
  - LLM does: summarization, natural language explanation, risk narrative ONLY

The agent NEVER infers relationships. It only explains what the graph found.
This is a critical architectural principle — the graph is authoritative,
the LLM is a communication layer.

Usage:
  pip install anthropic langgraph langchain-anthropic httpx python-dotenv
  python scripts/ecg_agent.py
  python scripts/ecg_agent.py --fund "Apex Senior Credit Fund III" --risk REGULATORY_WATCH
"""

import argparse
import json
import os
import httpx
from typing import TypedDict, Annotated
import operator

# LangGraph imports
try:
    from langgraph.graph import StateGraph, END
    from langgraph.checkpoint.memory import MemorySaver
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False
    print("⚠  langgraph not installed — running in simple mode")

# Anthropic SDK
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("⚠  anthropic not installed — LLM summaries disabled")

ECG_API_BASE = os.getenv("ECG_API_BASE", "http://localhost:5000")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-6"


# ─────────────────────────────────────────────────────────────────────────────
# Graph API client (calls the ECG .NET API — no direct Neo4j)
# ─────────────────────────────────────────────────────────────────────────────

class ECGGraphClient:
    """HTTP client for the ECG API. All structural data comes from graph traversal."""

    def __init__(self, base_url: str = ECG_API_BASE):
        self.base = base_url
        self.client = httpx.Client(timeout=30.0)

    def get_fund_exposure(self, fund_name: str, risk_type: str = "REGULATORY_WATCH", max_hops: int = 4) -> dict:
        url = f"{self.base}/api/funds/{httpx.URL(fund_name).path}/exposure"
        resp = self.client.get(url, params={"riskType": risk_type, "maxHops": max_hops})
        resp.raise_for_status()
        return resp.json()

    def get_ownership_chain(self, company_name: str) -> dict:
        resp = self.client.get(f"{self.base}/api/companies/{company_name}/ownership-chain")
        resp.raise_for_status()
        return resp.json()

    def get_risk_propagation(self, risk_id: str) -> dict:
        resp = self.client.get(f"{self.base}/api/risks/{risk_id}/propagation")
        resp.raise_for_status()
        return resp.json()

    def get_fund_portfolio(self, fund_name: str) -> dict:
        resp = self.client.get(f"{self.base}/api/funds/{fund_name}/portfolio")
        resp.raise_for_status()
        return resp.json()

    def get_graph_stats(self) -> dict:
        resp = self.client.get(f"{self.base}/api/graph/stats")
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self.client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Agent State
# ─────────────────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    fund_name: str
    risk_type: str
    max_hops: int
    # Graph results (populated by graph traversal nodes)
    graph_exposure: dict
    graph_portfolio: dict
    # Enriched data (populated after graph queries)
    enriched_context: dict
    # Final LLM output
    summary: str
    risk_narrative: str
    action_items: list[str]
    messages: Annotated[list, operator.add]


# ─────────────────────────────────────────────────────────────────────────────
# Agent nodes (LangGraph workflow steps)
# ─────────────────────────────────────────────────────────────────────────────

graph_client = ECGGraphClient()


def traverse_exposure_graph(state: AgentState) -> AgentState:
    """
    Step 1: Execute graph traversal — STRUCTURAL ONLY, no LLM.
    This is the most important step: the graph defines what's true.
    """
    print(f"\n[GRAPH] Traversing exposure chains: fund={state['fund_name']} risk={state['risk_type']}")

    try:
        exposure_data = graph_client.get_fund_exposure(
            fund_name=state["fund_name"],
            risk_type=state["risk_type"],
            max_hops=state["max_hops"]
        )
        print(f"[GRAPH] Found {exposure_data.get('exposureCount', 0)} exposure paths")
        print(f"[GRAPH] Traversal complete — passing {len(exposure_data.get('exposureChains', []))} chains to LLM")
    except Exception as e:
        print(f"[GRAPH] Error: {e}")
        exposure_data = {"exposureCount": 0, "exposureChains": [], "error": str(e)}

    return {**state, "graph_exposure": exposure_data}


def fetch_portfolio_context(state: AgentState) -> AgentState:
    """
    Step 2: Fetch full portfolio context from graph — still STRUCTURAL, no LLM.
    """
    print(f"\n[GRAPH] Fetching portfolio context for {state['fund_name']}...")

    try:
        portfolio = graph_client.get_fund_portfolio(state["fund_name"])
        position_count = portfolio.get("positionCount", 0)
        high_risk = portfolio.get("riskSummary", {}).get("highRisk", 0)
        print(f"[GRAPH] Portfolio: {position_count} positions, {high_risk} at HIGH risk")
    except Exception as e:
        print(f"[GRAPH] Portfolio fetch error: {e}")
        portfolio = {"positionCount": 0, "positions": [], "error": str(e)}

    return {**state, "graph_portfolio": portfolio}


def enrich_with_context(state: AgentState) -> AgentState:
    """
    Step 3: Compile enriched context from all graph results.
    No LLM. Pure data assembly.
    """
    print("\n[CONTEXT] Assembling enriched context from graph results...")

    exposure = state["graph_exposure"]
    portfolio = state["graph_portfolio"]

    chains = exposure.get("exposureChains", [])
    positions = portfolio.get("positions", [])

    # Extract key facts for LLM context
    high_severity_chains = [c for c in chains if c.get("severity") == "HIGH"]
    affected_entities = list({c["riskBearingEntity"] for c in chains})
    exposure_paths = [" → ".join(c.get("exposurePath", [])) for c in chains[:5]]
    total_exposure = sum(c.get("positionValueMillions", 0) for c in chains)
    unique_risks = list({c["riskId"] for c in chains})

    context = {
        "fund_name":           state["fund_name"],
        "risk_type":           state["risk_type"],
        "exposure_chain_count":len(chains),
        "high_severity_count": len(high_severity_chains),
        "total_exposure_mm":   round(total_exposure, 1),
        "affected_entities":   affected_entities,
        "exposure_paths":      exposure_paths,
        "unique_risk_ids":     unique_risks,
        "position_count":      portfolio.get("positionCount", 0),
        "aum_millions":        portfolio.get("aumMillions", 0),
        "risk_summary":        portfolio.get("riskSummary", {}),
        "raw_chains":          chains[:10],  # pass first 10 to LLM for narrative
    }

    print(f"[CONTEXT] Context assembled: {len(chains)} chains, {len(affected_entities)} affected entities")
    return {**state, "enriched_context": context}


def generate_risk_narrative(state: AgentState) -> AgentState:
    """
    Step 4: LLM generates a risk narrative — ONLY explains what the graph found.
    No inference of relationships. The LLM is a communication layer, not a knowledge source.
    """
    print("\n[LLM] Generating risk narrative from graph results...")

    context = state["enriched_context"]

    if not ANTHROPIC_AVAILABLE or not ANTHROPIC_API_KEY:
        # Fallback: template-based summary without LLM
        narrative = _template_narrative(context)
        action_items = _template_actions(context)
        return {
            **state,
            "summary": narrative,
            "risk_narrative": narrative,
            "action_items": action_items
        }

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""You are a capital markets risk analyst reviewing graph traversal results.
Your task is to explain what the graph analysis found in clear, factual language.

CRITICAL RULES:
1. Only explain what the graph data explicitly shows. Do NOT infer or hypothesize relationships.
2. Use precise financial terminology appropriate for a portfolio manager audience.
3. Cite specific entity names and hop counts from the data.
4. If the graph found no exposure paths, clearly state that.

GRAPH TRAVERSAL RESULTS:
{json.dumps(context, indent=2)}

Please provide:
1. EXECUTIVE SUMMARY (2-3 sentences): What indirect {context['risk_type']} exposure exists?
2. KEY FINDINGS (bullet points): Specific exposure paths with hop counts and entity names
3. RISK ASSESSMENT: Which findings are most material based on position size and severity?
4. RECOMMENDED ACTIONS (3-5 items): Specific, actionable next steps for the portfolio team

Format your response as JSON with keys: summary, key_findings (list), risk_assessment, action_items (list)
"""

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )

        content = response.content[0].text
        # Try to parse as JSON
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            # If not valid JSON, use as raw text
            parsed = {
                "summary":         content[:500],
                "key_findings":    [],
                "risk_assessment": content,
                "action_items":    []
            }

        print(f"[LLM] Narrative generated ({len(content)} chars)")
        return {
            **state,
            "summary":        parsed.get("summary", ""),
            "risk_narrative": parsed.get("risk_assessment", ""),
            "action_items":   parsed.get("action_items", []),
            "messages":       [{"role": "assistant", "content": content}]
        }

    except Exception as e:
        print(f"[LLM] Error: {e} — falling back to template")
        narrative = _template_narrative(context)
        return {
            **state,
            "summary":        narrative,
            "risk_narrative": narrative,
            "action_items":   _template_actions(context),
        }


def _template_narrative(ctx: dict) -> str:
    chain_count = ctx["exposure_chain_count"]
    high_count  = ctx["high_severity_count"]
    exposure_mm = ctx["total_exposure_mm"]
    fund        = ctx["fund_name"]
    risk_type   = ctx["risk_type"]

    if chain_count == 0:
        return f"Graph traversal found no indirect {risk_type} exposure paths for {fund}. Direct holdings appear clean."

    entities = ", ".join(ctx["affected_entities"][:3])
    return (
        f"Graph analysis identified {chain_count} indirect {risk_type} exposure paths for {fund}, "
        f"with {high_count} HIGH severity findings totalling ${exposure_mm:.1f}M at risk. "
        f"Risk-bearing entities include: {entities}."
    )


def _template_actions(ctx: dict) -> list[str]:
    items = []
    if ctx["exposure_chain_count"] > 0:
        items.append(f"Review {ctx['exposure_chain_count']} indirect exposure paths with deal team")
    if ctx["high_severity_count"] > 0:
        items.append(f"Escalate {ctx['high_severity_count']} HIGH-severity findings to Risk Committee")
    if ctx["total_exposure_mm"] > 100:
        items.append(f"Assess hedging options for ${ctx['total_exposure_mm']:.0f}M indirect exposure")
    items.append("Update investor quarterly risk report to include indirect exposure disclosure")
    items.append("Set monitoring alerts on affected entities in portfolio monitoring system")
    return items


# ─────────────────────────────────────────────────────────────────────────────
# Build LangGraph workflow
# ─────────────────────────────────────────────────────────────────────────────

def build_agent():
    if not LANGGRAPH_AVAILABLE:
        return None

    workflow = StateGraph(AgentState)

    workflow.add_node("traverse_exposure_graph",  traverse_exposure_graph)
    workflow.add_node("fetch_portfolio_context",  fetch_portfolio_context)
    workflow.add_node("enrich_with_context",      enrich_with_context)
    workflow.add_node("generate_risk_narrative",  generate_risk_narrative)

    # Linear pipeline: graph always runs before LLM
    workflow.set_entry_point("traverse_exposure_graph")
    workflow.add_edge("traverse_exposure_graph", "fetch_portfolio_context")
    workflow.add_edge("fetch_portfolio_context", "enrich_with_context")
    workflow.add_edge("enrich_with_context",     "generate_risk_narrative")
    workflow.add_edge("generate_risk_narrative",  END)

    return workflow.compile(checkpointer=MemorySaver())


# ─────────────────────────────────────────────────────────────────────────────
# Simple mode (without LangGraph)
# ─────────────────────────────────────────────────────────────────────────────

def run_simple_mode(fund_name: str, risk_type: str, max_hops: int):
    """Run the agent workflow without LangGraph for simpler deployments."""
    print("\n[AGENT] Running in simple mode (no LangGraph)\n")
    state = {
        "fund_name": fund_name,
        "risk_type": risk_type,
        "max_hops":  max_hops,
        "graph_exposure": {},
        "graph_portfolio": {},
        "enriched_context": {},
        "summary": "",
        "risk_narrative": "",
        "action_items": [],
        "messages": [],
    }
    state = traverse_exposure_graph(state)
    state = fetch_portfolio_context(state)
    state = enrich_with_context(state)
    state = generate_risk_narrative(state)
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ECG Intelligence Agent")
    parser.add_argument("--fund",     default="Apex Senior Credit Fund III")
    parser.add_argument("--risk",     default="REGULATORY_WATCH")
    parser.add_argument("--max-hops", type=int, default=4)
    args = parser.parse_args()

    print(f"\n{'='*65}")
    print("ECG Platform — Graph-Grounded Intelligence Agent")
    print(f"Fund:      {args.fund}")
    print(f"Risk Type: {args.risk}")
    print(f"Max Hops:  {args.max_hops}")
    print(f"LLM Model: {MODEL}")
    print(f"{'='*65}")

    # Check ECG API connectivity
    try:
        stats = graph_client.get_graph_stats()
        print(f"\n[API] Connected — {stats.get('totalNodes', '?')} nodes, "
              f"{stats.get('totalRelationships', '?')} relationships in graph")
    except Exception as e:
        print(f"\n[API] Warning: ECG API not reachable ({e})")
        print("   Make sure the API is running: docker-compose up ecg-api")
        print("   Continuing with mock data...\n")

    if LANGGRAPH_AVAILABLE:
        agent = build_agent()
        config = {"configurable": {"thread_id": "ecg-session-1"}}
        initial_state = AgentState(
            fund_name=args.fund,
            risk_type=args.risk,
            max_hops=args.max_hops,
            graph_exposure={},
            graph_portfolio={},
            enriched_context={},
            summary="",
            risk_narrative="",
            action_items=[],
            messages=[]
        )
        final_state = agent.invoke(initial_state, config=config)
    else:
        final_state = run_simple_mode(args.fund, args.risk, args.max_hops)

    # ── Output ───────────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print("AGENT OUTPUT")
    print(f"{'='*65}")

    ctx = final_state.get("enriched_context", {})
    print(f"\nFund:                {args.fund}")
    print(f"Risk Type:           {args.risk}")
    print(f"Exposure Paths:      {ctx.get('exposure_chain_count', 0)}")
    print(f"High Severity:       {ctx.get('high_severity_count', 0)}")
    print(f"Total Exposure:      ${ctx.get('total_exposure_mm', 0):.1f}M")
    print(f"Affected Entities:   {', '.join(ctx.get('affected_entities', []))}")

    if final_state.get("exposure_paths"):
        print("\nTop Exposure Paths (graph traversal output):")
        for path in ctx.get("exposure_paths", [])[:3]:
            print(f"  {path}")

    print(f"\nEXECUTIVE SUMMARY (LLM):")
    print(final_state.get("summary", "No summary generated"))

    if final_state.get("risk_narrative"):
        print(f"\nRISK ASSESSMENT:")
        print(final_state.get("risk_narrative"))

    if final_state.get("action_items"):
        print(f"\nACTION ITEMS:")
        for i, item in enumerate(final_state["action_items"], 1):
            print(f"  {i}. {item}")

    print(f"\n{'='*65}")
    print("ARCHITECTURE NOTE: Graph did structural traversal. LLM only explained results.")
    print("The LLM has no knowledge of ownership chains — the graph is the source of truth.")
    print(f"{'='*65}\n")

    graph_client.close()


if __name__ == "__main__":
    main()
