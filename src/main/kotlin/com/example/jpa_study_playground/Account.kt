package com.example.jpa_study_playground

import javax.persistence.Entity
import javax.persistence.GeneratedValue
import javax.persistence.Id

@Entity
class Account(
    @Id
    @GeneratedValue(strategy = javax.persistence.GenerationType.IDENTITY)
    val id : Long? = null,
    var balance : Long?,
)
