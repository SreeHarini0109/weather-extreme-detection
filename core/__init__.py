"""Shared domain logic used by every engine (sequential, multiprocessing, MPI, Spark).

Engines only decide *how work is split and combined*; all weather logic lives here so the
engines produce identical results and the timing comparison is fair.
"""
