import os

from cerebralcortex.CerebralCortex import CerebralCortex

configuration_file = os.path.join(os.path.dirname(__file__), '../cerebralcortex_apiserver.yml')

CC = CerebralCortex(configuration_file, master="local[*]", name="Memphis cStress Development App", time_zone="US/Central")
