from dataclasses import dataclass

import ahocorasick


@dataclass
class Dictionaries:
    ac_automaton:    ahocorasick.Automaton
    typo_list:       list[dict]
    typo_regex_list: list[dict]
    garbage_config:  dict
