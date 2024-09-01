import random
import re
import math
import pandas as pd
from datetime import datetime

# Global Constants
CHARACTER_SET = "FaizulNoorazreenaHamzahRamadhaniahHawaKhadijahSAWIYAHMDNOR1234567890!@#$%&?-"
CHARACTER_SET_SIZE = len(CHARACTER_SET)
ATTEMPTS_PER_SECOND = 10000
SPECIAL_CHARACTERS = "!@#$%&?-"
FILEPATH = 'D:/fmdnor/fmdnor/Kata Kunci/'
FILENAME = 'passwordlist.csv'

def save_password(my_password):
    try:
        pass_list = pd.read_csv(FILEPATH + FILENAME)
        pass_list.dropna(ignore_index=True, inplace=True)
    except FileNotFoundError:
        # If the file doesn't exist, create a new DataFrame
        pass_list = pd.DataFrame()

    now = datetime.now()
    t = now.strftime("%Y-%m-%d %H:%M:%S")
    username = input('Username: ')
    system_name = input('System/apps: ')

    data01 = {
        'username': username,
        'application': system_name,
        'password': my_password,
        'create on': t
    }

    # Append new data
    pass_list = pd.concat([pass_list, pd.DataFrame([data01])], ignore_index=True)

    # Save the updated data to the CSV file
    pass_list.to_csv(FILEPATH + FILENAME, index=False)

    print("Password information added successfully.")


def time_to_crack(password, character_set_size, attempts_per_second):
    yellow = "\033[93m"
    reset = "\033[0m"
    password_length = len(password)
    combinations = math.pow(character_set_size, password_length)
    time_seconds = combinations / attempts_per_second

    total_seconds = time_seconds

    # Calculate years
    years = total_seconds // (365.25 * 24 * 3600)
    remaining_seconds = total_seconds % (365.25 * 24 * 3600)

    # Calculate months
    months = remaining_seconds // (30.44 * 24 * 3600)
    remaining_seconds %= (30.44 * 24 * 3600)

    # Calculate days
    days = remaining_seconds // (24 * 3600)
    remaining_seconds %= (24 * 3600)

    # Calculate hours
    hours = remaining_seconds // 3600
    remaining_seconds %= 3600

    # Calculate minutes
    minutes = remaining_seconds // 60
    remaining_seconds %= 60

    # The remaining seconds
    seconds = remaining_seconds

    print(
        f"{yellow}Time to crack: {int(years)} years, {int(months)} months, {int(days)} days, {int(hours)} hours, {int(minutes)} minutes, and {int(seconds)} seconds{reset}")

    return time_seconds

def is_strong_password(password):
    # Check minimum length
    if len(password) < 8:
        return False

    # Check for at least one uppercase letter
    if not any(char.isupper() for char in password):
        return False

    # Check for at least one lowercase letter
    if not any(char.islower() for char in password):
        return False

    # Check for at least one digit
    if not any(char.isdigit() for char in password):
        return False

    # Check for at least one special character
    if not any(char in SPECIAL_CHARACTERS for char in password):
        return False

    # Check for consecutive characters (e.g., "abc", "123")
    if re.search(r'(.)\1', password):
        return False

    # Password passed all checks
    return True

def restart_script():
    print("\nThe selected password is weak. Restarting the script...\n")
    main()

def main():
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

    w = True
    while w:
        try:
            length = int(input('Password length: '))
            if 8 <= length <= 15:
                break
            else:
                print('Your password length should be between 8 - 15 characters long.')
        except ValueError:
            print('Please enter a valid integer for the password length.')

    n = True
    while n:
        try:
            number_of_passwords = int(input('Number of passwords to generate: '))
            if number_of_passwords <= 20:
                break
            else:
                print('Too many passwords to generate. Max = 20')
        except ValueError:
            print('Please enter a valid integer for the number of passwords.')

    print('\nPlease choose your option: \n')

    good_passwords = []

    for i in range(number_of_passwords):
        random_string = ''.join(random.choice(CHARACTER_SET) for _ in range(length))

        if is_strong_password(random_string):
            print(f"{GREEN}Option {i} recommended:{RESET} {random_string}")
            time_to_crack(random_string, CHARACTER_SET_SIZE, ATTEMPTS_PER_SECOND)
            good_passwords.append(random_string)

        else:
            print(f"{RED}Option {i} not recommended:{RESET} {random_string}")

    if len(good_passwords) > 0:
        selected_password = input('\nYour password: ')
        save_password(selected_password)
    else:
        restart_script()

if __name__ == "__main__":
    main()
