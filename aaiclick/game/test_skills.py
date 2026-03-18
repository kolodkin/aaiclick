from .skills import MAGE_SKILLS, SkillEffectType


def test_mage_level_2_skills_exist():
    assert 2 in MAGE_SKILLS
    assert len(MAGE_SKILLS[2]) == 2


def test_storm_skill():
    storm = next(s for s in MAGE_SKILLS[2] if s.name == "Storm")
    assert storm.name_he == "סופה"
    assert storm.level == 2
    assert len(storm.effects) == 1
    effect = storm.effects[0]
    assert effect.effect_type == SkillEffectType.DICE_PENALTY
    assert effect.value == 2


def test_dragon_breath_skill():
    dragon_breath = next(s for s in MAGE_SKILLS[2] if s.name == "Dragon Breath")
    assert dragon_breath.name_he == "נשימת דרקון"
    assert dragon_breath.level == 2
    assert len(dragon_breath.effects) == 1
    effect = dragon_breath.effects[0]
    assert effect.effect_type == SkillEffectType.NEUTRALIZE_ITEM
    assert effect.value == 1
