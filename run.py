# -*- coding: utf-8 -*-
"""
Нода для исполнения расчета
@author: V-Liderman
"""
#import io
import os
import sys
import getopt
from pyproc.core.shell import ProcShell
__BASE_DIR = os.path.dirname(__file__)
#sys.path.append(__RUN_DIR__)

HELP_STRING = """run.py

Использование:
    python run.py [options]

Среда исполнения ноды.

Опции:
    -i          название ini-файла
    -d          путь к папке с приложением
Дополнительные опции:
    --debug     Включить режим отладки
"""
# ==============================================================================
def main(*args):
    """Test/command line usage code.
    See command line usage help with::
      python run.py --help
    or::
        python -m run --help
    """
    if args and args[0].lower() in ("-h", "h", "--help", "-help", "help",
                                    "--aide", "-aide", "aide", "?"):
        print(HELP_STRING)
        sys.exit(0)

    try:
        optlist, args = getopt.getopt(args, 'id', ["version", "debug"])
    except getopt.GetoptError as err:
        print("Error,", err)
        print("See usage with: python treetaggerwrapper.py --help")
        sys.exit(-1)
    # Отработка параметров
    # режим отладки
    debug = False
    #название ini-файла
    ini_file = None
    #путь к освной директории исполнения
    base_dir = None
    for opt, val in optlist:
        if opt == "--debug":
            debug = True
        if opt == '-i':
            ini_file = val
        if opt == '-d':
            base_dir = val
    if not base_dir:
        base_dir = __BASE_DIR
    #/параметры
    #инициализируем shell
    shell = ProcShell(ini_file=ini_file, base_dir=base_dir, debug=False)#debug)
#    reader = None
#    reader = shell._data_adapter.fetch_to_dict
    #rec = shell._get_record('APP_Fetch_Next_Batch', {'@Node_id':1})#, reader)
#    rec = shell._get_record('APP_Calc_Subscr', {'@id': 389572, '@batch': 0}, reader)
#    if isinstance(rec, JsonTree):
#        rec = rec.json()
#    with open(r'C:\Users\v-liderman\Desktop\result2.json', 'w', encoding='utf-8') as fout:
#        json.dump(rec, fout, cls=CustomEncoder)
#        shell._data_adapter.execute('APP_Calc_Subscr', fout,  \
#                     param_values = {'@id':389572, '@batch':0}, verbose=True)

    #param_values=shell.context_param_values
    #shell._push_record('Text', 'APP_Append_Log', param_values)
#    shell._run_poll(1)
    shell.run(1)
#    param_values = {'@id':389572, '@batch':0}
#    shell._new_job(param_values)
    shell.stop()
    return 1


######################################################################################
if __name__ == "__main__":
    sys.exit(main(*(sys.argv[1:])))
