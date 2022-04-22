from types import SimpleNamespace
from .system import System
from .notifica import Notifica
from .sivep import Sivep
from .sim import Sim
from .gal import Gal
from .esus import eSUS
from .care import Care
from .casos_confirmados import CasosConfirmados

__all__ = [
    System,
    CasosConfirmados,
    Notifica,
    Sivep,
    Sim,
    Gal,
    eSUS,
    Care
]