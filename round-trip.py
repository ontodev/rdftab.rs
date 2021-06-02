#!/usr/bin/env python3

import argparse
import sys
from rdflib import Graph
from rdflib.compare import to_isomorphic, graph_diff


def compare_graphs(actual, expected, show_diff=False, sort=False):
    def dump_ttl(graph, sort):
        lines = graph.serialize(format="ttl").splitlines()
        if sort:
            lines.sort()
        for line in lines:
            if line:
                try:
                    print(line.decode("ascii"))
                except UnicodeDecodeError:
                    print(line)

    actual_iso = to_isomorphic(actual)
    expected_iso = to_isomorphic(expected)

    print("Comparing graphs ...")
    if actual_iso != expected_iso:
        print(
            "Graphs are not identical. Complete dumps of the actual and expected graphs can be "
            "found in build/actual.ttl and build/expected.ttl"
        )
        if show_diff:
            _, in_first, in_second = graph_diff(actual_iso, expected_iso)
            print("----- Contents of actual graph not in expected graph -----")
            dump_ttl(in_first, sort)
            print("----- Contents of expected graph not in actual graph -----")
            dump_ttl(in_second, sort)
        # Dump the complete graph contents to two files:
        with open("build/expected.ttl", "w") as fh:
            print(expected.serialize(format="n3").decode("utf-8"), file=fh)
        with open("build/actual.ttl", "w") as fh:
            print(actual.serialize(format="n3").decode("utf-8"), file=fh)
    else:
        print("Graphs are identical.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="round-trip.py",
        description="Reads a graph from STDIN and determines whether it is different from REF.",
    )
    parser.add_argument(
        "REF", type=argparse.FileType("r"), help="The reference file to be matched against."
    )
    args = parser.parse_args()

    actual = Graph()
    actual.parse(data="".join(sys.stdin.readlines()), format="ttl")
    expected = Graph()
    expected.parse(args.REF)
    compare_graphs(actual, expected, True)
