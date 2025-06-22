if __name__ == "__main__":
    import argparse
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep", type=int, default=20)
    parser.add_argument("--msg", type=str)
    args = parser.parse_args()

    def func(t: int, msg: str):
        time.sleep(t)
        print(msg)

    func(args.sleep, args.msg)
