CREATE TABLE IF NOT EXISTS consultations (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_code      VARCHAR(32)  NOT NULL,
    specialty         VARCHAR(100) NOT NULL,
    consultation_date DATE         NOT NULL,
    conclusion        TEXT         NOT NULL,
    recommendations   TEXT         NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT conclusion_min_len CHECK (char_length(conclusion) >= 20),
    CONSTRAINT patient_code_not_blank CHECK (length(btrim(patient_code)) > 0),
    CONSTRAINT specialty_not_blank CHECK (length(btrim(specialty)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_consultations_specialty_id
    ON consultations (specialty, id);
