using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
namespace CSVDataIngestionTest.Models
{
    
       



        public partial class Student
        {



            [RegularExpression(@"^STDN\d{5}$", ErrorMessage = "StudentID must be in STDN00000 format.")]
            public required string Student_ID { get; set; }

            // [Name("gender")]
            [RegularExpression(@"^[MF]$", ErrorMessage = "Gender must be 'M' or 'F'.")]
            public String? Gender { get; set; }

            // [Name("NationalITy")]
            [RegularExpression(@"^[a-zA-Z ]+$", ErrorMessage = "Nationality must only contain Alphabets and White Space.")]
            public string? Nationlity { get; set; }

            // [Name("PlaceofBirth")]
            [RegularExpression(@"^[a-zA-Z ]+$", ErrorMessage = "PlaceOfBirth must only contain Alphabets and White Space.")]
            public string? PlaceOfBirth { get; set; }

            //  [Name("StageID")]
            [RegularExpression("^(lowerlevel|MiddleSchool|HighSchool)$", ErrorMessage = "StageID must be 'loverlevel' or 'MiddleSchool' or 'HighSchool'.")]
            public string? StageID { get; set; }

            // [Name("GradeID")]
            [RegularExpression(@"^G-\d{2}$", ErrorMessage = "GradeID must be G-00 format.")]
            public string? GradeID { get; set; }

            // [Name("SectionID")]
            [RegularExpression(@"^[A-C]$", ErrorMessage = "SectionID must be 'A' or 'C'.")]
            public String? SectionID { get; set; }

            // [Name("Topic")]
            [RegularExpression(@"^[a-zA-Z]+$", ErrorMessage = "Topic must only contain Alphabets.")]
            public string? Topic { get; set; }

            // [Name("Semester")]
            [RegularExpression(@"^[FS]$", ErrorMessage = "Semester must be 'F' or 'S'.")]
            public String? Semester { get; set; }

            // [Name("Relation")]
            [RegularExpression("^(Father|Mum)$", ErrorMessage = "Relation must be 'Father' or 'Mum'.")]
            public string? Relation { get; set; }

            // [Name("raisedhands")]
            [RegularExpression(@"^(?:\d{1,3}|999)$", ErrorMessage = "RaisedHands must be a number between 0 - 999.")]
            public int? RaisedHands { get; set; }

            //   [Name("VisITedResources")]
            [RegularExpression(@"^(?:\d{1,3}|999)$", ErrorMessage = "VisitedResources must be a number between 0 - 999.")]
            public int? VisitedResources { get; set; }

            // [Name("AnnouncementsView")]
            [RegularExpression(@"^(?:\d{1,3}|999)$", ErrorMessage = "AnnouncementsView must be a number between 0 - 999.")]
            public int? AnnouncementsView { get; set; }

            // [Name("Discussion")]
            [RegularExpression(@"^(?:\d{1,3}|999)$", ErrorMessage = "Discussion must be a number between 0 - 999.")]
            public int? Discussion { get; set; }

            // [Name("ParentAnsweringSurvey")]
            [RegularExpression("^(Yes|No)$", ErrorMessage = "ParentAnsweringSurvey must be 'Yes' or 'No'.")]
            public string? ParentAnsweringSurvey { get; set; }

            // [Name("ParentschoolSatisfaction")]
            [RegularExpression("^(Good|Bad)$", ErrorMessage = "ParentSchoolSatisfaction must be 'Good' or 'Bad'.")]
            public string? ParentschoolSatisfaction { get; set; }

            // [Name("StudentAbsenceDays")]
            [RegularExpression("^(Under-7|Above-7)$", ErrorMessage = "StudentAbsenceDays must be 'Under-7' or 'Above-7'.")]
            public string? StudentAbsenceDays { get; set; }

            //  [Name("Student Marks")]
            [RegularExpression(@"^(?:\d{1,3}|100)$", ErrorMessage = "StudentMarks must be a number between 0 - 100.")]
            public int? StudentMarks { get; set; }

            // [Name("Class")]
            [RegularExpression(@"^[MLH]$", ErrorMessage = "Class must be 'M' or 'L' or 'H'.")]
            public string? Classes { get; set; }


        }

    }


