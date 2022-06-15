import logging
from typing import Iterable

from merico.analysis.analysis_db import IndicatorFactory
from merico.analysis.analyzer.aspect_graph_analyzer import CALL_ASPECT_GRAPH
from merico.graphserver.call_aspect_graph_constants import CAG_DATA_LANGUAGE
from merico.db.ca_report_models import RCagEdge

from merico.analysis.report import check_existence, sources

_logger = logging.getLogger(__name__)


@check_existence(CALL_ASPECT_GRAPH)
@sources(CALL_ASPECT_GRAPH)
def gen_cag_edges(indicator_factory: IndicatorFactory, cag_indicator,
                  analysis_uuid: str) -> Iterable[RCagEdge]:
    for _, cag in cag_indicator.graphs.items():
        for from_node_id, to_node_id, data in cag.edges(data=True):
            r_cag_edge: RCagEdge = RCagEdge()
            r_cag_edge.analysis_id = analysis_uuid
            r_cag_edge.language = data[CAG_DATA_LANGUAGE]
            r_cag_edge.from_node_id = from_node_id
            r_cag_edge.to_node_id = to_node_id
            yield r_cag_edge

