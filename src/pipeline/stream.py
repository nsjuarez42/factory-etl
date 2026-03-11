

from rx import create
from rx import operators as ops


def kafka_observable(consumer):
    def _observable(observer, _):
        try:
            for msg in consumer:
                observer.on_next(msg.value)
        except Exception as e:
            observer.on_error(e)
    return create(_observable)

def machine_codes():
    return {"UNS56A": "UNSCRAMBLER",
        "WS964F": "WASHER",
        "IS8710": "INSPECTION",
        "FB713A": "FILLING",
        "C7841R": "CARBONATOR",
        "CPM784": "CAPPING",
        "LBL74F": "LABELING",
        "PLL741": "PALLETIZER"
    }

def msg_attributes():
    return {"TS": "TIMESTAMP",
 "MC": "MACHINE",
 "PR": "PRODUCT",
 "PS": "PROPS"
            }
def properties_code():
    return{
 "A7": "LITERS",
 "W8": "QUALITY",
 "L1": "LIGHT",
 "T3": "TIME", 
 "P6": "POWER",
 "G8": "GRADES", 
    }

def machines_mapping(code):
    return machine_codes()[code]

def properties_mapping(code):
    return properties_code()[code]

def attributes_mapping(code):
    return msg_attributes()[code]

def auxa(event):
    event["MACHINE"] = machines_mapping(event["MACHINE"])
    return event

def auxb(event):
    print(f"b: {event}")

def auxp(event):
    to_delete = [] 
    for k,v in event["PS"].items():
        event["PS"][properties_mapping(k)] = v 
        to_delete.append(k)
    for k in to_delete:
        event["PS"].pop(k)
    return event


def build_pipeline(source, send_rich_event, save_raw_event, save_rich_event):
    return source.pipe(
        ops.do_action(lambda e: print(f"event received: {e}")),
        ops.do_action(save_raw_event),
        ops.filter(lambda e: e["MC"] in machine_codes().keys()),
        ops.map(lambda e:{attributes_mapping(k): e[k]  for k in e.keys()}),
        ops.do_action(lambda e: print(f"After attributes_mapping: {e}")),
        ops.map(lambda e: {**e,"MACHINE":machines_mapping(e["MACHINE"])}),
        ops.do_action(lambda e: print(f"After machine_mapping: {e}")),
        #ops.map(auxp),
        ops.map(lambda e: {**e,"PROPS":{properties_mapping(k):v for k,v in e["PROPS"].items()}}),
        ops.do_action(lambda e: print(f"After properties__mapping: {e}")),
        ops.do_action(save_rich_event),
        ops.do_action(send_rich_event),
        ops.do_action(lambda _: print("rich event send")),
    )
