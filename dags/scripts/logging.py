import logging

# Create a custom logger
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Create a console handler and set level to debug
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.INFO)

# Set the format for the console handler
c_format = logging.Formatter('%(levelname)s - %(message)s')
c_handler.setFormatter(c_format)

# Create a file handler and set level to debug
f_handler = logging.FileHandler('./test.log', mode='a')
f_handler.setLevel(logging.DEBUG)
# Set the format for the file handler
f_format = logging.Formatter('%(asctime)s - %(levelname)-8s [%(filename)s:%(lineno)d] - %(message)s \n', datefmt= '%d-%b-%y %H:%M:%S')
f_handler.setFormatter(f_format)

# Add the handler to the logger
log.addHandler(c_handler)
log.addHandler(f_handler)