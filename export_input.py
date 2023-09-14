from logging_export import logger

def user_input():
    while True:
        try:
            start_year, end_year = input("Please enter the range of year to be extracted (E.g, 2000-2012): ").split("-")
            start_year = int(start_year.strip())
            end_year = int(end_year.strip())
            if end_year < start_year:
                logger.info("Please ensure the given range of year is in the right order.")
            else:
                break
        
        except ValueError:
            logger.info("Sorry, your input is not following to the above mentioned format.")
            continue
        
    return start_year, end_year