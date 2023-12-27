# Current directory is brought to root level to avoid import issues
import subprocess
import sys

# get repo root level
root_path = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False
).stdout.rstrip("\n")
# add repo path to use all libraries
sys.path.append(root_path)

from configs import Config


def train(cfg):
    m_type = cfg.get('experiment/model/family')
    n_hidden_layers = cfg.get('experiment/model/n_layers', 36)

    assert m_type == 'resnet'
    print('True')
    assert n_hidden_layers == 151


if __name__ == '__main__':
    cfg = Config('../configs/config.yaml')
    train(cfg)
