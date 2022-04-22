from os.path import join
from bulletin import parent
from bulletin.utils import utils

if __name__ == '__main__':
    logger = utils.setup_logging(join(parent,'logging.yaml'),'bulletin')