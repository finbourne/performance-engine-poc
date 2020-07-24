import json
from config.config import PerformanceConfiguration

def test_config():
    PerformanceConfiguration.set_global_config(gs="a",gn=2)
    cfg = PerformanceConfiguration(ls="a2",ln=5)

    assert cfg.get('gs') == "a"
    assert cfg.get('gn') == 2
    assert cfg.get('ls') == "a2"
    assert cfg.get('ln') == 5
    assert cfg.get('na',-1) == -1
    assert cfg.ln == 5
    assert cfg.gn == 2
    assert PerformanceConfiguration.global_config.get('gs',5) == "a"
    assert PerformanceConfiguration.item('ls',"n/a") == "n/a"


def test_config_from_file(fs):

    # Create dummy config in the fake file-system
    with open("config.json","w") as fp:
        json.dump({"gn" : 123,"gs2" : "hello","ln" : -1},fp)

    PerformanceConfiguration.set_global_config(gs="a",gn=2,path="config.json")

    cfg = PerformanceConfiguration(ls="a2",ln=5)

    assert cfg.get('gs') == "a"
    assert cfg.get('gs2') == "hello"
    assert cfg.ln == 5
    assert cfg.ls == "a2"

    assert PerformanceConfiguration.item('ln') == -1
