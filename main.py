#!/usr/bin/python
import logging
import pandas as pd
from progressbar import progressbar
from typing import Optional, Union

logging.basicConfig(
    filename="4plebs-kewl-search.log",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
)


def main(
    filename: str, keywords: Union[list, tuple, set], errors: bool = True
) -> Optional[dict[set[int]]]:
    keywords_set = set(keywords)
    try:
        data = pd.read_csv(
            filename,
            names=(
                "num",
                "subnum",
                "thread_num",
                "op",
                "timestamp",
                "timestamp_expired",
                "preview_orig",
                "preview_w",
                "preview_h",
                "media_filename",
                "media_w",
                "media_h",
                "media_size",
                "media_hash",
                "media_orig",
                "spoiler",
                "deleted",
                "capcode",
                "email",
                "name",
                "trip",
                "title",
                "comment",
                "sticky",
                "locked",
                "poster_hash",
                "poster_country",
                "exif",
            ),
            usecols=("thread_num", "comment"),
            dtype={"thread_num": "float64", "comment": "object"},
            header=None,
            error_bad_lines=False,
            warn_bad_lines=True,
            verbose=True,
        )
    except FileNotFoundError:
        print("ERROR: Could not open " + filename)
        return None
    thread_set_dict = dict()
    for keyword in keywords_set:
        thread_set_dict[keyword] = set()
    for x in progressbar(
        data.itertuples(), max_value=data.shape[0], redirect_stdout=True
    ):
        try:
            thread_num, comment = x[1], x[2]
            if type(comment) is str:
                for keyword in keywords_set:
                    if keyword in comment:
                        thread_set_dict[keyword].add(int(thread_num))
        except IndexError:
            if errors is True:
                print("ERROR: " + str(x))
            logging.error("ERROR: " + str(x) + "\n")
    results = thread_set_dict
    logging.info("\n\nResults:\n" + str(results))
    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        filename = input("Please enter the path for the CSV data file: ")
    else:
        filename = sys.argv[1]

    if len(sys.argv) < 3:
        keywords = set()
        keyword_input = None
        while keyword_input != "":
            keyword_input = input(
                "Please enter a keyword (press enter without entering anything when done): "
            )
            if keyword_input != "":
                keywords.add(keyword_input)
    else:
        keywords = sys.argv[2 : len(sys.argv)]
    results = main(filename, keywords)
    if results is not None:
        print("\n\nResults:")
        for x in sorted(results):
            print(
                "Threads with “"
                + str(x)
                + "” ("
                + str(len(results[x]))
                + "): "
                + str(results[x])
            )
        print(
            "Threads with all keywords ("
            + str(len(set.intersection(*list(results.values()))))
            + "): "
            + str(set.intersection(*list(results.values())))
        )
