-- Add unique constraints on publishers.name and editors.name
CREATE UNIQUE INDEX "publishers_name_key" ON "publishers"("name");
CREATE UNIQUE INDEX "editors_name_key" ON "editors"("name");
