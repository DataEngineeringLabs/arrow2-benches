import json
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick


def set_size(width, fraction=1):
    """Set figure dimensions to avoid scaling in LaTeX.

    Parameters
    ----------
    width: float
            Document textwidth or columnwidth in pts
    fraction: float, optional
            Fraction of the width which you wish the figure to occupy

    Returns
    -------
    fig_dim: tuple
            Dimensions of figure in inches
    """
    # Width of figure (in pts)
    fig_width_pt = width * fraction

    # Convert from pt to inches
    inches_per_pt = 1 / 72.27

    # Golden ratio to set aesthetic figure height
    # https://disq.us/p/2940ij3
    golden_ratio = (5 ** 0.5 - 1) / 2

    # Figure width in inches
    fig_width_in = fig_width_pt * inches_per_pt
    # Figure height in inches
    fig_height_in = fig_width_in * golden_ratio

    fig_dim = (fig_width_in, fig_height_in)

    return fig_dim


def _read_reports(bench: str):
    root = f"target/criterion/{bench}/"

    result = []
    for original_path, dirs, files in os.walk(root):
        path = original_path.split(os.sep)
        if path[-1] != "new":
            continue
        path = path[-4:-1]
        task = path[0]
        type = path[1]
        size = int(path[2])

        with open(os.path.join(original_path, "estimates.json")) as f:
            data = json.load(f)

        ms = data["mean"]["point_estimate"] / 1000
        result.append(
            {
                "task": task,
                "type": type,
                "size": size,
                "time": ms,
            }
        )
    return result


def plot(result, choices, title, filename, to_stdout=False):
    x = [2 ** x["size"] for x in result if x["type"] == choices[0][0]]

    fig, ax = plt.subplots(1, 1, figsize=set_size(512))
    for (choice, name) in choices:
        values = [r["time"] for r in result if r["type"] == choice]
        ax.plot(x, values, "-o", label=name)

        if to_stdout:
            print(name)
            print("size, time (ms)")
            for (v1, v2) in zip(x, values):
                print(f"{v1}, {v2}")

    ax.set(xlabel="size", ylabel="time (ms)", title=title)
    ax.xaxis.set_major_formatter(mtick.ScalarFormatter(useMathText=True))
    ax.grid()
    ax.legend()

    fig.savefig(filename)


def _plot_sum():
    result = _read_reports("sum")

    plot(
        result,
        [
            ("native", "Vec<i32>"),
            ("arrow", "Arrow Array"),
            ("option", "Vec<Option<i32>>"),
        ],
        "Sum of integers",
        "sum_arrow.png",
    )

    plot(
        result,
        [
            ("native", "Vec<i32>"),
            ("arrow null", "Arrow Array"),
            ("option null", "Vec<Option<i32>>"),
        ],
        "Sum of nullable integers",
        "sum_null.png",
    )


result = (
    _read_reports("arrow_avro_read")
    + _read_reports("avro_read")
    + _read_reports("mz_avro_read")
    + _read_reports("fastavro")
)
for r in result:
    r["type"] = r["type"] + " " + r["task"]

plot(
    result,
    [
        ("utf8 arrow_avro_read", "arrow2"),
        ("utf8 mz_avro_read", "mz-avro"),
        ("utf8 avro_read", "avro"),
        ("utf8 fastavro", "fastavro (Python)"),
    ],
    "Read N rows of a single string column of 3 bytes each",
    "avro_read.png",
    True,
)

plot(
    result,
    [
        ("int arrow_avro_read", "arrow2"),
        ("int mz_avro_read", "mz-avro"),
        ("int avro_read", "avro"),
    ],
    "Read N rows of a single int column",
    "avro_read_int.png",
    True,
)

plot(
    result,
    [
        ("int deflate arrow_avro_read", "arrow2"),
        ("int deflate mz_avro_read", "mz-avro"),
        ("int deflate avro_read", "avro"),
    ],
    "Read N rows of a single int column",
    "avro_read_int_compressed.png",
    True,
)
