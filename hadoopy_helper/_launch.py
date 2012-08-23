class Freezer(object):

    def __init__(self, freeze_cache):
        try:
            os.makedirs(freeze_cache)
        except OSError:
            pass
        
        open()

    def launch_frozen(self, input_path, output_path, script_name, *kw, **args):
        pass
