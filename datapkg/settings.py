

def configure_logging(
        level='info',
        format='%(levelname)s:%(name)s:%(message)s'):
    """Get a logger with basic configurations."""
    import logging
    from importlib import reload
    reload(logging)
    level_dict = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR
    }
    logging.basicConfig(level=level_dict[level], format=format)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.debug('Done configuring logging!')


# def configure_logging(debug=False):
#     """Confugre logging for this module."""
#     LOGGING_CONFIGS = {
#         'version': 1,
#         'disable_existing_loggers': False,  # this fixes the problem
#
#         'formatters': {
#             'standard': {
#                 'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
#             },
#             'clean': {
#                 'format': '%(message)s',
#             },
#         },
#         'handlers': {
#             'default': {
#                 'level': 'DEBUG' if debug else 'INFO',
#                 'class': 'logging.StreamHandler',
#                 'formatter': 'clean',
#             },
#         },
#         'loggers': {
#             '': {
#                 'handlers': ['default'],
#                 'level': 'DEBUG' if debug else 'INFO',
#                 'propagate': True
#             }
#         }
#     }
#     logging.config.dictConfig(LOGGING_CONFIGS)
