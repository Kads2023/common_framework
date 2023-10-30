import time


def retry(delay=120, tries=4):
    def outer_func(func):
        def inner_func(*args, **kwargs):
            attempt = 0
            while attempt < tries:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    print(f'Exception thrown when attempting to run {func}, exc --> {exc}, '
                          f'attempt {attempt} of {tries}, will retry after waiting for {delay}')
                    attempt += 1
                    if attempt > tries:
                        print(f"More than {tries} attempts, closing the program")
                    time.sleep(delay)
            return func(*args, **kwargs)

        return inner_func

    return outer_func
