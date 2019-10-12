package utils


object Utils {

  def parseGender(g: String) = {
    g.toLowerCase match {
      case "male" | "m" | "male-ish" | "maile" |
           "mal" | "male (cis)" | "make" | "male " |
           "man" | "msle" | "mail" | "malr" |
           "cis man" | "cis male" => "Male"
      case "cis female" | "f" | "female" |
           "woman" |  "femake" | "female " |
           "cis-female/femme" | "female (cis)" |
           "femail" => "Female"
      case _ => "Transgender"
    }
  }

}
