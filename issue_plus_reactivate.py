from funcs import authenticate
from issue_credits import issue_main

def main():
    creds = authenticate()

    issue_main(creds)


if __name__ == "__main__":
    main()