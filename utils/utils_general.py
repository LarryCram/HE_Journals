import os


def cache_init(project_data_folder=None):
    """
    initialise the OA cache on data disk
    """

    # check that the disk path is correct by reading the identifying text file
    if os.path.exists(project_data_folder):
        return f"{project_data_folder}\\.oa"
    raise SystemExit(f'unable to locate project_data_folder {project_data_folder}\n'
                     f'cannot initialise .oa folder\n'
                     f'load the drive or change the name in {project_data_folder} in the project root folder')


def chained_get(container, path, default=None):
    """Helper function to perform a series of .get() methods on a dictionary
    or return the `default`.

    Parameters
    ----------
    container : dict
        The dictionary on which the .get() methods should be performed.

    path : list or tuple
        The list of keys that should be searched for.

    default : any (optional, default=None)
        The object type that should be returned if the search yields
        no result.
    """
    from functools import reduce

    # Obtain value via reduce
    try:
        return reduce(lambda c, k: c.get(k, default), path, container)
    except (AttributeError, TypeError):
        return default


def time_run(func: object) -> object:
    """
    wrapper for decorator to time func
    :param func:
    :return:
    """

    import datetime as dt

    def wrapper(*args, **kwargs):
        # Time stamp
        time_now = dt.datetime.now()
        time_start = time_now
        print(f'Run starts at {str(time_now).split(".")[0]}')
        result = func(*args, **kwargs)
        # Time stamp
        time_now = dt.datetime.now()
        duration = time_now - time_start
        minute, second = divmod(duration.total_seconds(), 60)
        print(f'NORMAL TERMINATION at {str(time_now).split(".")[0]} with elapsed time '
              f'{minute} m {second:.2f} s')
        return result

    return wrapper


def profile_run(func):
    """
    wrapper for decorator to run profile on func
    :param func:
    :return:
    """

    import cProfile
    import pstats
    from pstats import SortKey

    def wrapper(*args, **kwargs):
        func_name = f"{func.__name__}.pfl"
        prof = cProfile.Profile()
        retval = prof.runcall(func, *args, **kwargs)
        prof.dump_stats(func_name)
        p = pstats.Stats(func_name).strip_dirs()
        p.sort_stats(SortKey.CUMULATIVE).print_stats(15)
        return retval

    return wrapper

def exception_handler(func):
    """
    wrapper to avoid try-except blocks according to Medium article by Sivan Batra
    :param func:
    :return:
    """
    def inner_function(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            print(f"{e = }")
    return inner_function
