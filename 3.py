import logging
from typing import Iterable

from merico.analysis.analysis_db import IndicatorFactory
from merico.graphserver.build_graph.ccg_graph_analyzer import CALL_COMMIT_GRAPH
from merico.graphserver.call_commit_graph_constants import CCG_DATA_ADDED_BY
from merico.db.ca_report_models import RCcgEdge

from merico.analysis.report import check_existence, sources

_logger = logging.getLogger(__name__)


# TODO: Deprecated, EE doesn't use ccg_edges anymore. 2022-06-09
@check_existence(CALL_COMMIT_GRAPH)
@sources(CALL_COMMIT_GRAPH)
def gen_ccg_edges(indicator_factory: IndicatorFactory, ccg_indicator,
                  analysis_uuid: str) -> Iterable[RCcgEdge]:
    for language, ccg in ccg_indicator.graphs.items():
        for from_node_id, to_node_id, data in ccg.edges(data=True):
            r_ccg_edge: RCcgEdge = RCcgEdge()
            r_ccg_edge.analysis_id = analysis_uuid
            r_ccg_edge.language = language
            r_ccg_edge.from_node_id = from_node_id
            r_ccg_edge.to_node_id = to_node_id
            r_ccg_edge.added_by_hexsha = data[CCG_DATA_ADDED_BY]
            yield r_ccg_edge

