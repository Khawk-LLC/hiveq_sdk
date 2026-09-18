#!/usr/bin/env python3
"""Control who can see a function in the registry.

A function you push is readable by everyone in your organization by default.
This shows the other two cases: keeping one private, opening it to the whole
org later, and sharing it with one named teammate.

Run:  python functions_share.py
"""
import hiveq.flow as hf


def alpha_signal(values, window=20):
    """A toy signal — the point here is who can read it, not what it computes."""
    window_values = values[-window:]
    return sum(window_values) / len(window_values)


if __name__ == "__main__":
    # Publish it private: only you can see or load it.
    hf.push_function(alpha_signal, version="1.0.0", private=True)
    print("Pushed alpha_signal, private to you.")

    # Share it with one teammate — read only, and only this function. They can
    # load and run it; they cannot overwrite or delete it, and nothing else in
    # your namespace is exposed. Pass with_team=... or with_role=... instead to
    # share with a whole team or role.
    #
    #   hf.share_function("alpha_signal", with_user="<their user id>")

    # Or open it to the whole organization. No re-push needed — this flips the
    # existing function, every version of it.
    hf.set_function_visibility("alpha_signal", private=False)
    print("alpha_signal is now readable across your organization.")

    # See what others have shared with you.
    print("Shared with me:", hf.list_namespaces().get("shared_with_me", []))
