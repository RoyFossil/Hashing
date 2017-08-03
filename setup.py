import sys
from cx_Freeze import setup, Executable

setup(
	name = "Hashing",
	version = "2.2",
	description = "Extendible Hashing",
	executables = [Executable("hashing.py", base = "Console")])