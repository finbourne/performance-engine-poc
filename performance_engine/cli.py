from argparse import ArgumentParser
from argparse import REMAINDER
from lusidtools.lpt import lse
from lusidtools.lpt import lpt
from config.config import PerformanceConfiguration

from apis_performance.api import PerformanceApi

def parse(extend=None,args=None):

    psr = ArgumentParser('CLI',description="Performance CLI tool",fromfile_prefix_chars='@')
    cmds = psr.add_subparsers(title='Operations',dest='op')

    qry = cmds.add_parser('qry')
    
    qry.add_argument('scope')
    qry.add_argument('portfolio')
    qry.add_argument('from_date')
    qry.add_argument('to_date')
    qry.add_argument('--locked',action='store_true')
    qry.add_argument("--fields",nargs="*")
    qry.add_argument("--filename")
    qry.add_argument("--dfq",nargs=REMAINDER)
    qry.add_argument('--global-config',dest="config",default="config.json")

    post = cmds.add_parser('post')
    post.add_argument('scope')
    post.add_argument('portfolio')
    post.add_argument('date')
    post.add_argument('--asat',dest='post_asat')
    post.add_argument('--force')
    post.add_argument('--global-config',dest="config",default="config.json")

    per = cmds.add_parser('periods')
    per.add_argument('scope')
    per.add_argument('portfolio')
    per.add_argument('--global-config',dest="config",default="config.json")

    return psr.parse_args(args)

def process_args(api,args):

    PerformanceConfiguration.set_global_config(args.config)
    perf_api = PerformanceApi(api)

    if args.op == 'qry':
       return perf_api.performance_report(
              args.scope,
              args.portfolio,
              args.from_date,
              args.to_date,
              args.locked,
              args.fields)
    elif args.op == 'post':
       perf_api.lock_period(
               args.scope,
               args.portfolio,
               args.date,
               asat=args.post_asat)
    elif args.op == 'periods':
       return perf_api.get_periods(
                         args.scope,
                         args.portfolio)

def main():
    lpt.standard_flow(parse,lse.connect,process_args)

if __name__ == "__main__":
    main()
