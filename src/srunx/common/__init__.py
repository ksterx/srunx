"""Common foundation layer — config, exceptions, logging, version.

These modules have no dependencies on other srunx layers beyond
:mod:`srunx.domain` (via :mod:`srunx.common.config`, for
``ContainerResource`` defaults) and form the base that every other
layer is allowed to import.
"""
