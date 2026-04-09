from dataclasses import dataclass, asdict
from typing import Any

import ahocorasick


@dataclass
class Dictionaries:
    ac_automaton:    ahocorasick.Automaton
    typo_list:       list[dict]
    typo_regex_list: list[dict]
    garbage_config:  dict



@dataclass
class ErrorRecord:
    product_id:              str
    category:                str | None
    main_category:           str | None
    sub_category:            str | None
    product_brand:           str
    product_name_raw:        str
    product_name:            str
    product_ingredients_raw: str
    product_url:             str
    crawled_at:              Any   # pd.Timestamp | pd.NaT
    error_type:              str
    residual_text:           str

    def to_dict(self) -> dict:
        return asdict(self)