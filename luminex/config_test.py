import logging

from configs import load_cfg

cfg = load_cfg()
logger = logging.getLogger(__name__)

print(cfg["TEST"]["MODEL"]["FAMILY"])