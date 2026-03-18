from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SkillEffectType(str, Enum):
    DICE_PENALTY = "dice_penalty"       # Reduce opponent's dice bonus
    NEUTRALIZE_ITEM = "neutralize_item" # Neutralize one of opponent's items


@dataclass
class SkillEffect:
    effect_type: SkillEffectType
    value: int = 0  # Magnitude of the effect


@dataclass
class Skill:
    name: str
    name_he: str
    level: int
    description: str
    description_he: str
    effects: list[SkillEffect] = field(default_factory=list)


MAGE_SKILLS: dict[int, list[Skill]] = {
    2: [
        Skill(
            name="Storm",
            name_he="סופה",
            level=2,
            description="Causes the opponent to lose 2 dice bonuses (-2 to opponent's attacks).",
            description_he="גורם ליריב לאבד שני תוספים לקוביה (-2 להתקפות היריב).",
            effects=[
                SkillEffect(effect_type=SkillEffectType.DICE_PENALTY, value=2),
            ],
        ),
        Skill(
            name="Dragon Breath",
            name_he="נשימת דרקון",
            level=2,
            description="Neutralizes 1 item of the opponent.",
            description_he="מנטרל ליריב חפץ 1.",
            effects=[
                SkillEffect(effect_type=SkillEffectType.NEUTRALIZE_ITEM, value=1),
            ],
        ),
    ],
}
