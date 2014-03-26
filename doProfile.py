import pstats
import sys

stats = pstats.Stats(sys.argv[1])
#s=stats.strip_dirs()
s=stats
s=s.sort_stats('time')
s.print_stats(0.03)

