import dask.bag as db
from dask.distributed import Client
import sys

def count_words(file_path, word_length):
    client = Client()
    bag = db.read_text(file_path)
    words = bag.str.lower().str.replace("[^a-zA-Z0-9\\s]", "").str.split().flatten()
    words = words.filter(lambda x: len(x) >= word_length)
    word_counts = words.frequencies().compute()
    sorted_word_counts = sorted(word_counts, key=lambda x: x[1], reverse=True)
    print(sorted_word_counts)
    client.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main.py <file_path> <word_length>")
        sys.exit(1)

    file_path = sys.argv[1]
    word_length = int(sys.argv[2])

    count_words(file_path, word_length)
