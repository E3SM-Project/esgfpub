import sys

from warehouse import parse_args
from warehouse.report import Report, add_report_args, check_report_args
from warehouse.warehouse import AutoWarehouse
from warehouse.workflows.extraction import Extraction
from warehouse.workflows.cleanup import CleanUp
from warehouse.workflows.postprocess import PostProcess
from warehouse.workflows.publication import Publication
from warehouse.workflows.validation import Validation

subcommands = {
    'auto': AutoWarehouse,
    'report': Report,
    'extract': Extraction,
    'cleanup': CleanUp,
    'postprocess': PostProcess,
    'publication': Publication,
    'validation': Validation
}
arg_sources = [
    AutoWarehouse.add_args,
    add_report_args,
    Extraction.add_args,
    CleanUp.add_args,
    PostProcess.add_args,
    Publication.add_args,
    Validation.add_args
]
arg_checkers = {
    'auto': AutoWarehouse.arg_checker,
    'report': check_report_args,
    'extract': Extraction.arg_checker,
    'cleanup': CleanUp.arg_checker,
    'postprocess': PostProcess.arg_checker,
    'publication': Publication.arg_checker,
    'validation': Validation.arg_checker
}


def main():
    args = parse_args(arg_sources, arg_checkers)
    if not args:
        return -1
    command = args.subparser_name
    job = subcommands[command](**vars(args))
    return job()


if __name__ == "__main__":
    sys.exit(main())