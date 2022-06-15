import logging
from datetime import date
from typing import Iterable

from merico.analysis.analysis_db import IndicatorFactory
from merico.analysis.analyzer.commit_file_label_analyzer import (
    COMMIT_LABELED_FILES,
    CommitLabeledFile,
)
from merico.analysis.analyzer.filter_analyzer import IGNORED_RECORDS_IN_HISTORY
from merico.analysis.concern.filter import FilteredRecord

from merico.db.starrocks_models import File
from merico.analysis.report import check_existence, sources

_logger = logging.getLogger(__name__)

COMMIT_REASONS = {"CommitBlacklist", "TooManyFilesChangedCommit"}


@check_existence(IGNORED_RECORDS_IN_HISTORY)
@sources(IGNORED_RECORDS_IN_HISTORY, COMMIT_LABELED_FILES)
def gen_files(
    indicator_factory: IndicatorFactory,
    ignored_records_i,
    commit_labeled_files_i,
    analysis_id: str,
    project_id: int,
    dt: date,
) -> Iterable[File]:
    record: FilteredRecord
    for record in ignored_records_i.get_list():
        if record.reason in COMMIT_REASONS:
            continue

        file = File()
        file.analysis_id = analysis_id
        file.project_id = project_id
        file.dt = dt
        file.hexsha = record.hexsha
        file.file_path = record.filepath
        file.ignored = record.reason
        yield file

    a_file: CommitLabeledFile
    for a_file in commit_labeled_files_i.get_list():
        file = File()
        file.analysis_id = analysis_id
        file.project_id = project_id
        file.dt = dt
        file.hexsha = a_file.hexsha
        file.file_path = a_file.file_path
        file.pojo = a_file.is_pojo
        yield file

