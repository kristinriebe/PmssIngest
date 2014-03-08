use TestDB;
create table Particles (
    particleId  bigint not null,
    x  float not null,
    y  float not null,
    z  float not null,
    vx  float not null,
    vy  float not null,
    vz  float not null,
    phkey int
) ENGINE=MyISAM;
