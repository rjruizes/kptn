from kptn.util.compute import compute_resource_requirements


def test_compute_requirements_convert_cpu_units_and_memory():
    requirements = compute_resource_requirements({"cpu": 1024, "memory": 2048})

    assert requirements == [
        {"type": "VCPU", "value": "1"},
        {"type": "MEMORY", "value": "2048"},
    ]


def test_compute_requirements_handles_fractional_cpu():
    requirements = compute_resource_requirements({"cpu": 0.25})

    assert requirements == [{"type": "VCPU", "value": "0.25"}]
